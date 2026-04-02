# src/etl/load_dim_achievement.py
"""
Carga dim_achievement usando GetSchemaForGame.
Solo carga logros de juegos que están en bibliotecas de usuarios.
Guarda progreso en data/cache/achievement_progress.json.

Fuente: ISteamUserStats/GetSchemaForGame/v2
"""
import json
import logging
import time
from pathlib import Path

import requests
from sqlalchemy import text

from src.config import get_settings
from src.db import get_session, start_etl_run, finish_etl_run
from src.etl.collect_user_libraries import get_unique_appids_from_jsonl

logger = logging.getLogger(__name__)
settings = get_settings()

SCHEMA_URL       = "https://api.steampowered.com/ISteamUserStats/GetSchemaForGame/v2/"
DELAY_BETWEEN    = 0.5
DELAY_ON_429     = 30
CHECKPOINT_EVERY = 200

PROGRESS_PATH = Path(__file__).resolve().parents[2] / "data" / "cache" / "achievement_progress.json"

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


# ── SQL ───────────────────────────────────────────────────────────────────────

SQL_GET_GAME_KEY = text("""
    SELECT game_key FROM dim_game
    WHERE appid = :appid AND is_current = 1
""")

SQL_EXISTS = text("""
    SELECT achievement_key FROM dim_achievement
    WHERE game_key = :game_key
      AND achievement_api_name = :achievement_api_name
""")

SQL_INSERT = text("""
    INSERT INTO dim_achievement (
        game_key, achievement_api_name, achievement_display_name,
        achievement_description, is_hidden, default_value,
        icon_url, icon_gray_url, etl_run_id
    )
    VALUES (
        :game_key, :achievement_api_name, :achievement_display_name,
        :achievement_description, :is_hidden, :default_value,
        :icon_url, :icon_gray_url, :etl_run_id
    )
""")

SQL_UPDATE = text("""
    UPDATE dim_achievement
    SET achievement_display_name = :achievement_display_name,
        achievement_description  = :achievement_description,
        is_hidden                = :is_hidden,
        icon_url                 = :icon_url,
        icon_gray_url            = :icon_gray_url,
        updated_at               = GETDATE(),
        etl_run_id               = :etl_run_id
    WHERE achievement_key = :achievement_key
""")


# ── API ───────────────────────────────────────────────────────────────────────

def get_game_schema(appid: int, request_number: int = 0, retry: bool = True) -> dict | None:
    api_key = _get_key(request_number)
    try:
        r = requests.get(
            SCHEMA_URL,
            params={
                "key":    api_key,
                "appid":  appid,
                "l":      settings.steam_lang,
                "format": "json",
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
                return get_game_schema(appid, request_number + 1, retry=False)
            return None

        if r.status_code in (400, 403, 404):
            return None

        r.raise_for_status()
        data = r.json()
        return data.get("game", {}).get("availableGameStats", {})

    except requests.exceptions.ConnectionError:
        logger.warning(f"  Error de red appid {appid}, esperando 15s...")
        time.sleep(15)
        if retry:
            return get_game_schema(appid, request_number, retry=False)
        return None

    except requests.RequestException as e:
        logger.warning(f"  Error appid {appid}: {e}")
        return None


# ── Load ──────────────────────────────────────────────────────────────────────

def safe_truncate(s: str, max_chars: int) -> str:
    """Trunca string garantizando que no excede max_chars caracteres."""
    if not s:
        return ""
    return s[:max_chars]

def load() -> None:
    run_id   = start_etl_run("load_dim_achievement")
    inserted = 0
    updated  = 0
    skipped  = 0
    games_with_achievements = 0

    try:
        # Paso 1: appids de bibliotecas
        logger.info("Leyendo appids desde user_libraries.jsonl...")
        appids = get_unique_appids_from_jsonl()
        logger.info(f"  {len(appids):,} appids únicos.")

        # Paso 2: game_map en memoria
        logger.info("Cargando game_map en memoria...")
        with get_session() as session:
            rows = session.execute(
                text("SELECT appid, game_key FROM dim_game WHERE is_current = 1")
            ).fetchall()
        game_map = {row.appid: row.game_key for row in rows}

        # Paso 3: filtrar pendientes
        done_appids = load_progress()
        pending     = [a for a in appids if a not in done_appids]
        logger.info(
            f"  Ya procesados: {len(done_appids):,} | "
            f"Pendientes: {len(pending):,}"
        )
        logger.info(f"  Keys activas: {len(_API_KEYS)}")

        if not pending:
            logger.info("  Nada pendiente.")
            finish_etl_run(run_id, status="success")
            return

        # Paso 4: obtener esquemas de logros
        req_counter = 0

        for i, appid in enumerate(pending, 1):
            game_key = game_map.get(appid)
            if not game_key:
                skipped += 1
                done_appids.add(appid)
                continue

            schema = get_game_schema(appid, req_counter)
            req_counter += 1

            if not schema:
                skipped += 1
                done_appids.add(appid)
                time.sleep(DELAY_BETWEEN)
                continue

            achievements = schema.get("achievements", [])
            if not achievements:
                skipped += 1
                done_appids.add(appid)
                time.sleep(DELAY_BETWEEN)
                continue

            games_with_achievements += 1

            with get_session() as session:
                for ach in achievements:
                    api_name = (ach.get("name") or "").strip()
                    if not api_name:
                        continue

                    row = {
                        "game_key":                game_key,
                        "achievement_api_name":     api_name,
                        "achievement_display_name": safe_truncate((ach.get("displayName") or api_name).strip(), 100),
                        "achievement_description":  safe_truncate((ach.get("description") or "").strip(), 300),
                        "is_hidden":                1 if ach.get("hidden", 0) else 0,
                        "default_value":            int(ach.get("defaultvalue", 0)),
                        "icon_url":                 safe_truncate((ach.get("icon") or "").strip(), 500),
                        "icon_gray_url":            safe_truncate((ach.get("icongray") or "").strip(), 500),
                        "etl_run_id":               run_id,
                    }

                    existing = session.execute(SQL_EXISTS, {
                        "game_key":             game_key,
                        "achievement_api_name": api_name,
                    }).fetchone()

                    if existing:
                        session.execute(SQL_UPDATE, {
                            **row,
                            "achievement_key": existing.achievement_key,
                        })
                        updated += 1
                    else:
                        session.execute(SQL_INSERT, row)
                        inserted += 1

            done_appids.add(appid)

            if i % CHECKPOINT_EVERY == 0:
                save_progress(done_appids)
                logger.info(
                    f"  [{i:,}/{len(pending):,}] "
                    f"juegos_con_logros={games_with_achievements:,} "
                    f"insertados={inserted:,} actualizados={updated:,} "
                    f"skip={skipped:,}"
                )

            time.sleep(DELAY_BETWEEN)

        # Checkpoint final
        save_progress(done_appids)

        finish_etl_run(
            run_id, status="success",
            rows_inserted=inserted,
            rows_updated=updated,
            rows_skipped=skipped,
        )
        logger.info(
            f"✅ dim_achievement completado:"
            f"\n  Juegos con logros:   {games_with_achievements:,}"
            f"\n  Logros insertados:   {inserted:,}"
            f"\n  Logros actualizados: {updated:,}"
            f"\n  Saltados:            {skipped:,}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise
    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()