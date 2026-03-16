# src/etl/load_dim_user.py
"""
ETL para dim_user con SCD Tipo 2.
Fuentes: BFS de amigos + grupo de Steam.
Checkpoint cada 100 usuarios para resistir interrupciones.
"""
import hashlib
import logging
import time
from datetime import datetime

from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run
from src.etl.steam_users import (
    collect_all_steamids,
    get_player_summaries,
    load_progress,
    save_progress,
)
from src.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

CHECKPOINT_EVERY = 100  # guardar progreso cada N usuarios


# ── Helpers ───────────────────────────────────────────────────────────────────

def hash_steamid(steamid: str) -> str:
    """Hash SHA-256 del steamid para anonimización."""
    return hashlib.sha256(steamid.encode()).hexdigest()


def get_age_band(account_created_date) -> str | None:
    """Calcula la franja de antigüedad de la cuenta."""
    if not account_created_date:
        return None
    now = datetime.now()
    years = (now - datetime.fromtimestamp(account_created_date)).days / 365
    if years < 1:
        return "0-1 año"
    elif years < 3:
        return "1-3 años"
    elif years < 5:
        return "3-5 años"
    elif years < 10:
        return "5-10 años"
    else:
        return "10+ años"


def extract_user_row(player: dict, country_key_map: dict, time_bucket_map: dict, run_id: int) -> dict:
    """Extrae los campos de dim_user desde un perfil de GetPlayerSummaries."""
    steamid = player.get("steamid", "")
    timecreated = player.get("timecreated")
    lastlogoff = player.get("lastlogoff")
    loccountrycode = player.get("loccountrycode", "")

    # Franja horaria del último logoff
    last_bucket_key = None
    if lastlogoff:
        hour = datetime.fromtimestamp(lastlogoff).hour
        if 0 <= hour < 6:
            last_bucket_key = time_bucket_map.get("madrugada")
        elif 6 <= hour < 12:
            last_bucket_key = time_bucket_map.get("mañana")
        elif 12 <= hour < 18:
            last_bucket_key = time_bucket_map.get("tarde")
        else:
            last_bucket_key = time_bucket_map.get("noche")

    created_date = None
    created_year = None
    if timecreated:
        dt = datetime.fromtimestamp(timecreated)
        created_date = dt.strftime("%Y-%m-%d")
        created_year = dt.year

    return {
        "steamid_hash":                hash_steamid(steamid),
        "visibility_state":            player.get("communityvisibilitystate"),
        "profile_state":               player.get("profilestate"),
        "persona_state":               player.get("personastate"),
        "country_key":                 country_key_map.get(loccountrycode),
        "account_created_date":        created_date,
        "account_created_year":        created_year,
        "account_age_band":            get_age_band(timecreated),
        "last_logoff_date":            datetime.fromtimestamp(lastlogoff).strftime("%Y-%m-%d") if lastlogoff else None,
        "last_logoff_time_bucket_key": last_bucket_key,
        "etl_run_id":                  run_id,
    }


# ── SQL ───────────────────────────────────────────────────────────────────────

SQL_GET_COUNTRY_MAP = text("SELECT iso_code, country_key FROM dim_country")
SQL_GET_BUCKET_MAP  = text("SELECT bucket_name, time_bucket_key FROM dim_time_bucket")

SQL_GET_CURRENT = text("""
    SELECT user_key, visibility_state, profile_state, persona_state,
           country_key, account_created_date, account_created_year,
           account_age_band, last_logoff_date, last_logoff_time_bucket_key
    FROM dim_user
    WHERE steamid_hash = :steamid_hash AND is_current = 1
""")

SQL_CLOSE_VERSION = text("""
    UPDATE dim_user
    SET valid_to   = GETDATE(),
        is_current = 0,
        updated_at = GETDATE(),
        etl_run_id = :etl_run_id
    WHERE user_key = :user_key
""")

SQL_INSERT = text("""
    INSERT INTO dim_user (
        steamid_hash, visibility_state, profile_state, persona_state,
        country_key, account_created_date, account_created_year,
        account_age_band, last_logoff_date, last_logoff_time_bucket_key,
        etl_run_id, valid_from, valid_to, is_current, is_active
    )
    VALUES (
        :steamid_hash, :visibility_state, :profile_state, :persona_state,
        :country_key, :account_created_date, :account_created_year,
        :account_age_band, :last_logoff_date, :last_logoff_time_bucket_key,
        :etl_run_id, GETDATE(), NULL, 1, 1
    )
""")

SCD2_FIELDS = [
    "visibility_state", "profile_state", "persona_state",
    "country_key", "last_logoff_date", "last_logoff_time_bucket_key",
    "account_age_band",
]


def has_changed(current_row, new_row: dict) -> bool:
    for field in SCD2_FIELDS:
        db_val  = getattr(current_row, field, None)
        new_val = new_row.get(field)
        if isinstance(db_val, bool):
            db_val = 1 if db_val else 0
        if db_val != new_val:
            return True
    return False


# ── Load ──────────────────────────────────────────────────────────────────────

def load() -> None:
    run_id = start_etl_run("load_dim_user")
    inserted = 0
    updated  = 0
    skipped  = 0

    try:
        # Cargar mapas de referencia
        with get_session() as session:
            country_map = {
                row.iso_code: row.country_key
                for row in session.execute(SQL_GET_COUNTRY_MAP)
            }
            bucket_map = {
                row.bucket_name: row.time_bucket_key
                for row in session.execute(SQL_GET_BUCKET_MAP)
            }

        logger.info(f"  {len(country_map)} países y {len(bucket_map)} buckets cargados.")

        # Recolectar todos los steamids (con checkpoint)
        steamids = collect_all_steamids(settings.steam_id64)
        total = len(steamids)
        logger.info(f"\nProcesando {total} usuarios en dim_user...")

        # Cargar progreso de usuarios ya insertados
        progress = load_progress()
        already_processed = set(progress.get("users_inserted", []))
        pending = [s for s in steamids if s not in already_processed]
        logger.info(f"  {len(already_processed)} ya procesados, {len(pending)} pendientes.")

        # Procesar en lotes de 100 (límite de GetPlayerSummaries)
        batch_size = 100
        for i in range(0, len(pending), batch_size):
            batch_ids = pending[i:i + batch_size]
            players   = get_player_summaries(batch_ids)

            with get_session() as session:
                for player in players:
                    steamid   = player.get("steamid", "")
                    steam_hash = hash_steamid(steamid)
                    new_row   = extract_user_row(player, country_map, bucket_map, run_id)

                    current = session.execute(
                        SQL_GET_CURRENT, {"steamid_hash": steam_hash}
                    ).fetchone()

                    if current is None:
                        session.execute(SQL_INSERT, new_row)
                        inserted += 1
                    elif has_changed(current, new_row):
                        session.execute(SQL_CLOSE_VERSION, {
                            "user_key":  current.user_key,
                            "etl_run_id": run_id,
                        })
                        session.execute(SQL_INSERT, new_row)
                        updated += 1
                    else:
                        skipped += 1

            # Checkpoint cada CHECKPOINT_EVERY usuarios
            already_processed.update(batch_ids)
            progress["users_inserted"] = list(already_processed)
            save_progress(progress)

            processed = min(i + batch_size, len(pending))
            logger.info(
                f"  [{processed}/{len(pending)}] "
                f"insertados={inserted} actualizados={updated} skip={skipped}"
            )
            time.sleep(0.5)

        finish_etl_run(
            run_id, status="success",
            rows_inserted=inserted,
            rows_updated=updated,
            rows_skipped=skipped,
        )
        logger.info(
            f"dim_user: {inserted} insertados, "
            f"{updated} actualizados, {skipped} sin cambios."
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()