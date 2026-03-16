# src/etl/load_fact_user_owned_game.py
"""
Carga fact_user_owned_game desde user_libraries.jsonl.
Granularidad: 1 fila por user_key + game_key + date_key (fecha de snapshot).

No depende de enrich_dim_game: solo necesita que el juego
exista en dim_game (ya poblado por SteamSpy) y el usuario en dim_user.
"""
import hashlib
import json
import logging
from datetime import datetime, date
from pathlib import Path

from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run

logger = logging.getLogger(__name__)

LIBRARIES_CACHE = Path(__file__).resolve().parents[2] / "data" / "cache" / "user_libraries.jsonl"
CHECKPOINT_EVERY = 500

TODAY_DATE_KEY = int(datetime.now().strftime("%Y%m%d"))


# ── SQL ───────────────────────────────────────────────────────────────────────

SQL_GET_USER_KEY = text("""
    SELECT user_key FROM dim_user
    WHERE steamid_hash = :steamid_hash AND is_current = 1
""")

SQL_GET_GAME_KEY = text("""
    SELECT game_key FROM dim_game
    WHERE appid = :appid AND is_current = 1
""")

SQL_GET_BUCKET_MAP = text("""
    SELECT time_bucket_key, bucket_name FROM dim_time_bucket
""")

SQL_MERGE = text("""
    MERGE fact_user_owned_game AS target
    USING (VALUES (
        :user_key, :game_key, :date_key,
        :playtime_forever_min, :playtime_windows_forever_min,
        :playtime_mac_forever_min, :playtime_linux_forever_min,
        :playtime_deck_forever_min, :rtime_last_played_date,
        :rtime_last_played_bucket_key, :has_visible_stats,
        :has_leaderboards, :has_workshop, :has_market,
        :has_dlc, :playtime_disconnected_min, :etl_run_id
    )) AS source (
        user_key, game_key, date_key,
        playtime_forever_min, playtime_windows_forever_min,
        playtime_mac_forever_min, playtime_linux_forever_min,
        playtime_deck_forever_min, rtime_last_played_date,
        rtime_last_played_bucket_key, has_visible_stats,
        has_leaderboards, has_workshop, has_market,
        has_dlc, playtime_disconnected_min, etl_run_id
    )
    ON target.user_key = source.user_key
   AND target.game_key = source.game_key
   AND target.date_key = source.date_key
    WHEN MATCHED THEN
        UPDATE SET
            playtime_forever_min         = source.playtime_forever_min,
            playtime_windows_forever_min = source.playtime_windows_forever_min,
            playtime_mac_forever_min     = source.playtime_mac_forever_min,
            playtime_linux_forever_min   = source.playtime_linux_forever_min,
            playtime_deck_forever_min    = source.playtime_deck_forever_min,
            rtime_last_played_date       = source.rtime_last_played_date,
            rtime_last_played_bucket_key = source.rtime_last_played_bucket_key,
            has_visible_stats            = source.has_visible_stats,
            has_leaderboards             = source.has_leaderboards,
            has_workshop                 = source.has_workshop,
            has_market                   = source.has_market,
            has_dlc                      = source.has_dlc,
            playtime_disconnected_min    = source.playtime_disconnected_min,
            etl_run_id                   = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (
            user_key, game_key, date_key,
            playtime_forever_min, playtime_windows_forever_min,
            playtime_mac_forever_min, playtime_linux_forever_min,
            playtime_deck_forever_min, rtime_last_played_date,
            rtime_last_played_bucket_key, has_visible_stats,
            has_leaderboards, has_workshop, has_market,
            has_dlc, playtime_disconnected_min, etl_run_id
        )
        VALUES (
            source.user_key, source.game_key, source.date_key,
            source.playtime_forever_min, source.playtime_windows_forever_min,
            source.playtime_mac_forever_min, source.playtime_linux_forever_min,
            source.playtime_deck_forever_min, source.rtime_last_played_date,
            source.rtime_last_played_bucket_key, source.has_visible_stats,
            source.has_leaderboards, source.has_workshop, source.has_market,
            source.has_dlc, source.playtime_disconnected_min, source.etl_run_id
        );
""")

SQL_GET_ALL_GAME_KEYS = text("""
    SELECT appid, game_key FROM dim_game WHERE is_current = 1
""")

SQL_GET_ALL_USER_KEYS = text("""
    SELECT steamid_hash, user_key FROM dim_user WHERE is_current = 1
""")

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_time_bucket_key(timestamp: int, bucket_map: dict) -> int | None:
    """Convierte un unix timestamp a time_bucket_key."""
    if not timestamp:
        return None
    hour = datetime.fromtimestamp(timestamp).hour
    if 0 <= hour < 6:
        return bucket_map.get("madrugada")
    elif 6 <= hour < 12:
        return bucket_map.get("mañana")
    elif 12 <= hour < 18:
        return bucket_map.get("tarde")
    else:
        return bucket_map.get("noche")


def get_last_played_date(timestamp: int) -> str | None:
    if not timestamp:
        return None
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")


# ── Load ──────────────────────────────────────────────────────────────────────

def load() -> None:
    run_id   = start_etl_run("load_fact_user_owned_game")
    inserted = 0
    skipped  = 0

    try:
        if not LIBRARIES_CACHE.exists():
            logger.error("No se encontró user_libraries.jsonl.")
            finish_etl_run(run_id, status="failed",
                           error_message="user_libraries.jsonl no existe")
            return

        # Cargar TODOS los mapas en memoria de una vez → 100x más rápido
        logger.info("Cargando mapas de referencia en memoria...")
        with get_session() as session:
            # appid → game_key
            game_map = {
                row.appid: row.game_key
                for row in session.execute(SQL_GET_ALL_GAME_KEYS).fetchall()
            }
            # steamid_hash → user_key
            user_map = {
                row.steamid_hash: row.user_key
                for row in session.execute(SQL_GET_ALL_USER_KEYS).fetchall()
            }
            # bucket_name → time_bucket_key
            bucket_map = {
                row.bucket_name: row.time_bucket_key
                for row in session.execute(SQL_GET_BUCKET_MAP).fetchall()
            }

        logger.info(f"  game_map: {len(game_map):,} juegos")
        logger.info(f"  user_map: {len(user_map):,} usuarios")
        logger.info(f"  date_key del snapshot: {TODAY_DATE_KEY}")

        user_count = 0
        batch_rows = []  # acumular filas para insertar en lote

        with open(LIBRARIES_CACHE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                for steam_hash, user_data in entry.items():
                    games = user_data.get("games", [])

                    user_key = user_map.get(steam_hash)
                    if not user_key:
                        skipped += 1
                        continue

                    for game in games:
                        appid    = game.get("appid")
                        game_key = game_map.get(appid)

                        if not appid or not game_key:
                            skipped += 1
                            continue

                        rtime = game.get("rtime_last_played", 0)

                        batch_rows.append({
                            "user_key":                    user_key,
                            "game_key":                    game_key,
                            "date_key":                    TODAY_DATE_KEY,
                            "playtime_forever_min":        game.get("playtime_forever", 0),
                            "playtime_windows_forever_min": game.get("playtime_windows_forever", 0),
                            "playtime_mac_forever_min":    game.get("playtime_mac_forever", 0),
                            "playtime_linux_forever_min":  game.get("playtime_linux_forever", 0),
                            "playtime_deck_forever_min":   game.get("playtime_deck_forever", 0),
                            "rtime_last_played_date":      get_last_played_date(rtime),
                            "rtime_last_played_bucket_key": get_time_bucket_key(rtime, bucket_map),
                            "has_visible_stats":           1 if game.get("has_visible_stats") else 0,
                            "has_leaderboards":            1 if game.get("has_leaderboards") else 0,
                            "has_workshop":                1 if game.get("has_workshop") else 0,
                            "has_market":                  1 if game.get("has_market") else 0,
                            "has_dlc":                     1 if game.get("has_dlc") else 0,
                            "playtime_disconnected_min":   game.get("playtime_disconnected", 0),
                            "etl_run_id":                  run_id,
                        })

                    user_count += 1

                    # Insertar en lotes de 1000 filas
                    if len(batch_rows) >= 1000:
                        with get_session() as session:
                            session.execute(SQL_MERGE, batch_rows)
                        inserted += len(batch_rows)
                        batch_rows = []

                        if user_count % CHECKPOINT_EVERY == 0:
                            logger.info(
                                f"  [{user_count:,} usuarios] "
                                f"filas={inserted:,} skip={skipped:,}"
                            )

        # Insertar filas restantes
        if batch_rows:
            with get_session() as session:
                session.execute(SQL_MERGE, batch_rows)
            inserted += len(batch_rows)

        finish_etl_run(
            run_id, status="success",
            rows_inserted=inserted,
            rows_skipped=skipped,
        )
        logger.info(
            f"✅ fact_user_owned_game completado:"
            f"\n  Usuarios procesados: {user_count:,}"
            f"\n  Filas insertadas:    {inserted:,}"
            f"\n  Saltadas:            {skipped:,}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()