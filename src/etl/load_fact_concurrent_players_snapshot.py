# src/etl/load_fact_concurrent_players_snapshot.py
"""
Carga fact_concurrent_players_snapshot usando GetNumberOfCurrentPlayers.
Solo procesa juegos que están en bibliotecas de usuarios.
Guarda progreso en data/cache/ccu_progress.json.

Fuente: ISteamUserStats/GetNumberOfCurrentPlayers/v1
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

CCU_URL          = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
DELAY_BETWEEN    = 0.5
DELAY_ON_429     = 30
CHECKPOINT_EVERY = 1000

PROGRESS_PATH = Path(__file__).resolve().parents[2] / "data" / "cache" / "ccu_progress.json"

_API_KEYS = [
    k for k in [
        settings.steam_api_key,
        settings.steam_api_key_2,
        settings.steam_api_key_3,
        settings.steam_api_key_4,
        settings.steam_api_key_5,
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

SQL_INSERT = text("""
    INSERT INTO fact_concurrent_players_snapshot
        (game_key, captured_at, date_key, current_player_count, etl_run_id)
    VALUES
        (:game_key, :captured_at, :date_key, :current_player_count, :etl_run_id)
""")


# ── API ───────────────────────────────────────────────────────────────────────

def get_ccu(appid: int, request_number: int = 0, retry: bool = True) -> int | None:
    try:
        r = requests.get(
            CCU_URL,
            params={"appid": appid, "format": "json"},
            timeout=15,
        )

        if r.status_code == 429:
            if retry:
                logger.warning(f"  429 appid {appid}, esperando {DELAY_ON_429}s...")
                time.sleep(DELAY_ON_429)
                return get_ccu(appid, request_number + 1, retry=False)
            return None

        if r.status_code in (400, 403, 404):
            return None

        r.raise_for_status()
        result = r.json().get("response", {})
        if result.get("result") != 1:
            return None
        return result.get("player_count", 0)

    except requests.exceptions.ConnectionError:
        logger.warning(f"  Error de red appid {appid}, esperando 15s...")
        time.sleep(15)
        if retry:
            return get_ccu(appid, request_number, retry=False)
        return None

    except requests.RequestException as e:
        logger.warning(f"  Error appid {appid}: {e}")
        return None


# ── Load ──────────────────────────────────────────────────────────────────────

def load() -> None:
    run_id   = start_etl_run("load_fact_concurrent_players_snapshot")
    inserted = 0
    skipped  = 0

    try:
        # Paso 1: appids de bibliotecas
        logger.info("Leyendo appids desde user_libraries.jsonl...")
        appids = get_unique_appids_from_jsonl()
        logger.info(f"  {len(appids):,} appids únicos en bibliotecas.")

        # Paso 2: cargar game_key map en memoria
        logger.info("Cargando game_map en memoria...")
        with get_session() as session:
            rows = session.execute(
                text("SELECT appid, game_key FROM dim_game WHERE is_current = 1")
            ).fetchall()
        game_map = {row.appid: row.game_key for row in rows}
        logger.info(f"  {len(game_map):,} juegos en game_map.")

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

        # Paso 4: capturar CCU
        captured_at = datetime.now()
        date_key    = int(captured_at.strftime("%Y%m%d"))
        batch       = []
        req_counter = 0

        for i, appid in enumerate(pending, 1):
            game_key = game_map.get(appid)
            if not game_key:
                skipped += 1
                done_appids.add(appid)
                continue

            ccu = get_ccu(appid, req_counter)
            req_counter += 1

            if ccu is not None:
                batch.append({
                    "game_key":             game_key,
                    "captured_at":          captured_at,
                    "date_key":             date_key,
                    "current_player_count": ccu,
                    "etl_run_id":           run_id,
                })
            else:
                skipped += 1

            done_appids.add(appid)

            # Insertar en lotes de 500
            if len(batch) >= 500:
                with get_session() as session:
                    session.execute(SQL_INSERT, batch)
                inserted += len(batch)
                batch = []

            # Checkpoint cada 1000
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
            f"✅ fact_concurrent_players_snapshot completado:"
            f"\n  Snapshots insertados: {inserted:,}"
            f"\n  Sin datos (skip):     {skipped:,}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()