# src/etl/enrich_dim_game.py
"""
Enriquece dim_game con datos de appdetails para los juegos
que están en bibliotecas de usuarios y tienen campos NULL.

Flujo:
  1. Lee appids únicos desde user_libraries.jsonl
  2. Filtra los que tienen game_type IS NULL en dim_game
  3. Llama appdetails por cada uno (con rate limit y rotación de keys)
  4. UPDATE con SCD2 si cambió algo, UPDATE in place si solo tenía NULLs

Fuente: store.steampowered.com/api/appdetails
"""
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import requests
from sqlalchemy import text

from src.config import get_settings
from src.db import get_session, start_etl_run, finish_etl_run
from src.etl.collect_user_libraries import get_unique_appids_from_jsonl

logger = logging.getLogger(__name__)
settings = get_settings()

DETAILS_URL        = "https://store.steampowered.com/api/appdetails"
DELAY_BETWEEN_REQS = 1.5
DELAY_ON_429       = 60
CHECKPOINT_EVERY   = 500

PROGRESS_PATH = Path(__file__).resolve().parents[2] / "data" / "cache" / "enrich_progress.json"

# Rotación de keys igual que en collect_user_libraries
_API_KEYS = [
    k for k in [
        settings.steam_api_key,
        settings.steam_api_key_2,
        settings.steam_api_key_3,
        settings.steam_api_key_4,
        settings.steam_api_key_5,
        settings.steam_api_key_6,
    ]
    if k
]

def _get_key(n: int) -> str:
    return _API_KEYS[n % len(_API_KEYS)]


# ── Progreso ──────────────────────────────────────────────────────────────────

def _atomic_save(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        tmp.replace(path)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def load_progress() -> set:
    if not PROGRESS_PATH.exists() or PROGRESS_PATH.stat().st_size == 0:
        return set()
    try:
        with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
            return set(json.load(f).get("done_appids", []))
    except json.JSONDecodeError:
        return set()


def save_progress(done: set) -> None:
    _atomic_save(PROGRESS_PATH, {"done_appids": list(done)})


# ── API ───────────────────────────────────────────────────────────────────────

def get_appdetails(appid: int, request_number: int = 0, retry: bool = True) -> dict | None:
    api_key = _get_key(request_number)
    try:
        r = requests.get(
            DETAILS_URL,
            params={
                "appids": appid,
                "cc":     settings.steam_country,
                "l":      settings.steam_lang,
            },
            timeout=15,
        )

        if r.status_code == 429:
            if retry:
                logger.warning(
                    f"  429 appid {appid} "
                    f"(key ...{api_key[-6:]}), esperando {DELAY_ON_429}s..."
                )
                time.sleep(DELAY_ON_429)
                return get_appdetails(appid, request_number + 1, retry=False)
            return None

        if r.status_code in (400, 403, 404):
            return None

        r.raise_for_status()
        data     = r.json()
        app_data = data.get(str(appid), {})

        if not app_data.get("success"):
            return None

        return app_data.get("data", {})

    except requests.exceptions.ConnectionError:
        logger.warning(f"  Error de red appid {appid}, esperando 30s...")
        time.sleep(30)
        if retry:
            return get_appdetails(appid, request_number, retry=False)
        return None

    except requests.RequestException as e:
        logger.warning(f"  Error appid {appid}: {e}")
        return None


# ── Transform ─────────────────────────────────────────────────────────────────

def parse_release_date(date_str: str | None) -> str | None:
    if not date_str:
        return None
    for fmt in ("%d %b, %Y", "%b %Y", "%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def extract_enriched_fields(data: dict) -> dict:
    release      = data.get("release_date", {})
    metacritic   = data.get("metacritic", {})
    recommendations = data.get("recommendations", {})
    platforms    = data.get("platforms", {})

    return {
        "game_name":           (data.get("name") or "").strip()[:255] or "Unknown",
        "game_type":           (data.get("type") or "").strip()[:50] or None,
        "required_age":        int(data.get("required_age") or 0),
        "is_free":             1 if data.get("is_free") else 0,
        "controller_support":  (data.get("controller_support") or "").strip()[:50] or None,
        "website":             (data.get("website") or "").strip()[:500] or None,
        "release_date":        parse_release_date(release.get("date")),
        "coming_soon":         1 if release.get("coming_soon") else 0,
        "recommendations_total": recommendations.get("total"),
        "achievements_total":  data.get("achievements", {}).get("total"),
        "metacritic_score":    metacritic.get("score"),
        "platform_windows":    1 if platforms.get("windows") else 0,
        "platform_mac":        1 if platforms.get("mac") else 0,
        "platform_linux":      1 if platforms.get("linux") else 0,
    }


# ── SQL ───────────────────────────────────────────────────────────────────────

SQL_GET_NEEDS_ENRICHMENT = text("""
    SELECT game_key, appid, game_name,
           game_type, release_date, platform_windows
    FROM dim_game
    WHERE appid IN :appids
      AND is_current = 1
      AND game_type IS NULL
""")

SQL_GET_CURRENT = text("""
    SELECT game_key, game_name, game_type, required_age, is_free,
           controller_support, website, release_date, coming_soon,
           recommendations_total, achievements_total, metacritic_score,
           platform_windows, platform_mac, platform_linux
    FROM dim_game
    WHERE appid = :appid AND is_current = 1
""")

SQL_CLOSE_VERSION = text("""
    UPDATE dim_game
    SET valid_to   = GETDATE(),
        is_current = 0,
        updated_at = GETDATE(),
        etl_run_id = :etl_run_id
    WHERE game_key = :game_key
""")

SQL_INSERT_NEW_VERSION = text("""
    INSERT INTO dim_game (
        appid, game_name, game_type, required_age, is_free,
        controller_support, website, release_date, coming_soon,
        recommendations_total, achievements_total, metacritic_score,
        platform_windows, platform_mac, platform_linux,
        last_modified_ts, price_change_number,
        etl_run_id, valid_from, valid_to, is_current, is_active
    )
    VALUES (
        :appid, :game_name, :game_type, :required_age, :is_free,
        :controller_support, :website, :release_date, :coming_soon,
        :recommendations_total, :achievements_total, :metacritic_score,
        :platform_windows, :platform_mac, :platform_linux,
        NULL, NULL,
        :etl_run_id, GETDATE(), NULL, 1, 1
    )
""")

SQL_UPDATE_IN_PLACE = text("""
    UPDATE dim_game
    SET game_name             = :game_name,
        game_type             = :game_type,
        required_age          = :required_age,
        is_free               = :is_free,
        controller_support    = :controller_support,
        website               = :website,
        release_date          = :release_date,
        coming_soon           = :coming_soon,
        recommendations_total = :recommendations_total,
        achievements_total    = :achievements_total,
        metacritic_score      = :metacritic_score,
        platform_windows      = :platform_windows,
        platform_mac          = :platform_mac,
        platform_linux        = :platform_linux,
        updated_at            = GETDATE(),
        etl_run_id            = :etl_run_id
    WHERE game_key = :game_key
""")

SCD2_FIELDS = [
    "game_name", "game_type", "required_age", "is_free",
    "controller_support", "website", "release_date", "coming_soon",
    "recommendations_total", "achievements_total", "metacritic_score",
    "platform_windows", "platform_mac", "platform_linux",
]


def has_changed(current_row, new_fields: dict) -> bool:
    for field in SCD2_FIELDS:
        db_val  = getattr(current_row, field, None)
        new_val = new_fields.get(field)

        # Si el nuevo valor es None o vacío, no considerar como cambio
        if new_val is None or new_val == "":
            continue

        if isinstance(db_val, bool):
            db_val = 1 if db_val else 0
        if isinstance(db_val, str):
            db_val = db_val.strip()
        if isinstance(new_val, str):
            new_val = new_val.strip()
        if db_val != new_val:
            return True
    return False


# ── Load ──────────────────────────────────────────────────────────────────────

def load() -> None:
    run_id   = start_etl_run("enrich_dim_game")
    enriched = 0
    updated  = 0
    skipped  = 0

    try:
        # ── Paso 1: appids de bibliotecas ─────────────────────────────────────
        logger.info("Leyendo appids desde user_libraries.jsonl...")
        appids_from_libraries = get_unique_appids_from_jsonl()
        logger.info(f"  {len(appids_from_libraries)} appids únicos en bibliotecas.")

        # ── Paso 2: filtrar los que necesitan enriquecimiento ─────────────────
        logger.info("Identificando juegos con game_type IS NULL...")

        # Convertir a set para búsqueda rápida
        appids_set = set(appids_from_libraries)

        # Traer todos los juegos con NULL de dim_game y filtrar en Python
        needs_enrichment = []
        with get_session() as session:
            rows = session.execute(text("""
                SELECT game_key, appid, game_name, game_type
                FROM dim_game
                WHERE is_current = 1
                AND game_type IS NULL
            """)).fetchall()

            for row in rows:
                if row.appid in appids_set:
                    needs_enrichment.append(row)

        total = len(needs_enrichment)
        logger.info(f"  {total} juegos en bibliotecas necesitan enriquecimiento.")

        # ── Paso 3: cargar progreso anterior ─────────────────────────────────
        done_appids = load_progress()
        pending     = [r for r in needs_enrichment if r.appid not in done_appids]
        logger.info(
            f"  Ya enriquecidos: {len(done_appids)} | "
            f"Pendientes: {len(pending)}"
        )
        logger.info(f"  Keys activas: {len(_API_KEYS)} → throughput x{len(_API_KEYS)}")

        # ── Paso 4: enriquecer ────────────────────────────────────────────────
        request_counter = 0

        for i, row in enumerate(pending, 1):
            appid = row.appid

            data = get_appdetails(appid, request_counter)
            request_counter += 1

            if not data:
                skipped += 1
                done_appids.add(appid)
                time.sleep(DELAY_BETWEEN_REQS)
                continue

            new_fields = extract_enriched_fields(data)

            with get_session() as session:
                current = session.execute(
                    SQL_GET_CURRENT, {"appid": appid}
                ).fetchone()

                if current is None:
                    skipped += 1
                else:
                    if has_changed(current, new_fields):
                        session.execute(SQL_CLOSE_VERSION, {
                            "game_key":  current.game_key,
                            "etl_run_id": run_id,
                        })
                        session.execute(SQL_INSERT_NEW_VERSION, {
                            "appid": appid,
                            **new_fields,
                            "etl_run_id": run_id,
                        })
                        updated += 1
                    else:
                        session.execute(SQL_UPDATE_IN_PLACE, {
                            "game_key": current.game_key,
                            **new_fields,
                            "etl_run_id": run_id,
                        })
                        enriched += 1

            done_appids.add(appid)

            if i % CHECKPOINT_EVERY == 0:
                save_progress(done_appids)
                logger.info(
                    f"  [{i}/{len(pending)}] "
                    f"enriquecidos={enriched} actualizados={updated} "
                    f"skip={skipped}"
                )

            time.sleep(DELAY_BETWEEN_REQS)

        # Checkpoint final
        save_progress(done_appids)

        finish_etl_run(
            run_id, status="success",
            rows_inserted=updated,
            rows_updated=enriched,
            rows_skipped=skipped,
        )
        logger.info(
            f"✅ enrich_dim_game completado:"
            f"\n  Enriquecidos (update in place): {enriched}"
            f"\n  Actualizados (SCD2):            {updated}"
            f"\n  Saltados:                       {skipped}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()