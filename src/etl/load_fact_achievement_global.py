# src/etl/load_fact_achievement_global.py
"""
Carga fact_achievement_global usando GetGlobalAchievementPercentagesForApp.
Solo procesa juegos que tienen logros en dim_achievement.
Guarda progreso en data/cache/achievement_global_progress.json.

Fuente: ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2
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

logger = logging.getLogger(__name__)
settings = get_settings()

GLOBAL_ACH_URL   = "https://api.steampowered.com/ISteamUserStats/GetGlobalAchievementPercentagesForApp/v2/"
DELAY_BETWEEN    = 1.0
DELAY_ON_429     = 30
CHECKPOINT_EVERY = 200

PROGRESS_PATH = Path(__file__).resolve().parents[2] / "data" / "cache" / "achievement_global_progress.json"


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

SQL_GET_GAMES_WITH_ACHIEVEMENTS = text("""
    SELECT DISTINCT g.appid, g.game_key
    FROM dim_game g
    INNER JOIN dim_achievement a ON a.game_key = g.game_key
    WHERE g.is_current = 1
      AND g.is_active  = 1
      AND a.is_active  = 1
""")

SQL_GET_ACHIEVEMENT_MAP = text("""
    SELECT achievement_key, game_key, achievement_api_name
    FROM dim_achievement
    WHERE game_key = :game_key
      AND is_active = 1
""")

SQL_INSERT = text("""
    MERGE fact_achievement_global AS target
    USING (VALUES (
        :achievement_key, :game_key, :date_key,
        :global_unlock_percent, :etl_run_id
    )) AS source (
        achievement_key, game_key, date_key,
        global_unlock_percent, etl_run_id
    )
    ON target.achievement_key = source.achievement_key
   AND target.date_key        = source.date_key
    WHEN MATCHED THEN
        UPDATE SET
            global_unlock_percent = source.global_unlock_percent,
            etl_run_id            = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (
            achievement_key, game_key, date_key,
            global_unlock_percent, etl_run_id
        )
        VALUES (
            source.achievement_key, source.game_key, source.date_key,
            source.global_unlock_percent, source.etl_run_id
        );
""")


# ── API ───────────────────────────────────────────────────────────────────────

def get_global_percentages(appid: int, retry: bool = True) -> list[dict] | None:
    """
    Obtiene los porcentajes globales de desbloqueo de logros.
    Devuelve lista de {name, percent} o None si hay error.
    """
    try:
        r = requests.get(
            GLOBAL_ACH_URL,
            params={
                "gameid": appid,
                "format": "json",
            },
            timeout=15,
        )

        if r.status_code == 429:
            if retry:
                logger.warning(f"  429 appid {appid}, esperando {DELAY_ON_429}s...")
                time.sleep(DELAY_ON_429)
                return get_global_percentages(appid, retry=False)
            return None

        if r.status_code in (400, 403, 404):
            return None

        r.raise_for_status()
        data = r.json()
        achievements = (
            data.get("achievementpercentages", {})
                .get("achievements", [])
        )
        return achievements if achievements else None

    except requests.exceptions.ConnectionError:
        logger.warning(f"  Error de red appid {appid}, esperando 15s...")
        time.sleep(15)
        if retry:
            return get_global_percentages(appid, retry=False)
        return None

    except requests.RequestException as e:
        logger.warning(f"  Error appid {appid}: {e}")
        return None


# ── Load ──────────────────────────────────────────────────────────────────────

def load() -> None:
    run_id   = start_etl_run("load_fact_achievement_global")
    inserted = 0
    skipped  = 0

    try:
        # Paso 1: obtener juegos que tienen logros en dim_achievement
        logger.info("Obteniendo juegos con logros en dim_achievement...")
        with get_session() as session:
            games = session.execute(SQL_GET_GAMES_WITH_ACHIEVEMENTS).fetchall()

        total = len(games)
        logger.info(f"  {total:,} juegos con logros a procesar.")

        if total == 0:
            logger.warning("  dim_achievement está vacía. Ejecuta load_dim_achievement primero.")
            finish_etl_run(run_id, status="failed",
                           error_message="dim_achievement vacía")
            return

        # Paso 2: filtrar pendientes
        done_appids = load_progress()
        pending     = [g for g in games if g.appid not in done_appids]
        logger.info(
            f"  Ya procesados: {len(done_appids):,} | "
            f"Pendientes: {len(pending):,}"
        )

        if not pending:
            logger.info("  Nada pendiente.")
            finish_etl_run(run_id, status="success")
            return

        # Paso 3: procesar cada juego
        date_key = int(datetime.now().strftime("%Y%m%d"))
        batch    = []

        for i, game_row in enumerate(pending, 1):
            appid    = game_row.appid
            game_key = game_row.game_key

            # Obtener porcentajes globales de la API
            percentages = get_global_percentages(appid)

            if not percentages:
                skipped += 1
                done_appids.add(appid)
                time.sleep(DELAY_BETWEEN)
                continue

            # Construir mapa api_name → percent
            pct_map = {
                p.get("name", "").strip(): float(p.get("percent", 0))
                for p in percentages
                if p.get("name")
            }

            # Obtener logros del juego desde dim_achievement
            with get_session() as session:
                ach_rows = session.execute(
                    SQL_GET_ACHIEVEMENT_MAP, {"game_key": game_key}
                ).fetchall()

            matched = 0
            for ach in ach_rows:
                pct = pct_map.get(ach.achievement_api_name)
                if pct is None:
                    continue

                batch.append({
                    "achievement_key":      ach.achievement_key,
                    "game_key":             game_key,
                    "date_key":             date_key,
                    "global_unlock_percent": round(pct, 2),
                    "etl_run_id":           run_id,
                })
                matched += 1

            if matched == 0:
                skipped += 1

            done_appids.add(appid)

            # Insertar en lotes de 500
            if len(batch) >= 500:
                with get_session() as session:
                    session.execute(SQL_INSERT, batch)
                inserted += len(batch)
                batch = []

            # Checkpoint cada 200 juegos
            if i % CHECKPOINT_EVERY == 0:
                save_progress(done_appids)
                logger.info(
                    f"  [{i:,}/{len(pending):,}] "
                    f"insertados={inserted:,} skip={skipped:,}"
                )

            time.sleep(DELAY_BETWEEN)

        # Insertar restantes
        if batch:
            with get_session() as session:
                session.execute(SQL_INSERT, batch)
            inserted += len(batch)

        # Checkpoint final
        save_progress(done_appids)

        finish_etl_run(
            run_id, status="success",
            rows_inserted=inserted,
            rows_skipped=skipped,
        )
        logger.info(
            f"✅ fact_achievement_global completado:"
            f"\n  Porcentajes insertados: {inserted:,}"
            f"\n  Juegos sin datos:       {skipped:,}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()
