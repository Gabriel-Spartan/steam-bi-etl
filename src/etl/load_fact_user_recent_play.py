# src/etl/load_fact_user_recent_play.py
"""
Carga fact_user_recent_play usando GetRecentlyPlayedGames.
Granularidad: 1 fila por user_key + game_key + date_key.
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

RECENT_PLAY_URL  = "https://api.steampowered.com/IPlayerService/GetRecentlyPlayedGames/v1/"
USERS_PROGRESS   = Path(__file__).resolve().parents[2] / "data" / "cache" / "users_progress.json"
TODAY_DATE_KEY   = int(datetime.now().strftime("%Y%m%d"))
DELAY_BETWEEN    = 1.0
CHECKPOINT_EVERY = 500

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


SQL_GET_ALL_GAME_KEYS = text("SELECT appid, game_key FROM dim_game WHERE is_current = 1")
SQL_GET_USER          = text("""
    SELECT user_key, steamid_hash FROM dim_user WHERE is_current = 1
""")

SQL_MERGE = text("""
    MERGE fact_user_recent_play AS target
    USING (VALUES (
        :user_key, :game_key, :date_key,
        :playtime_2weeks_min, :playtime_forever_min,
        :playtime_windows_forever_min, :playtime_mac_forever_min,
        :playtime_linux_forever_min, :playtime_deck_forever_min,
        :etl_run_id
    )) AS source (
        user_key, game_key, date_key,
        playtime_2weeks_min, playtime_forever_min,
        playtime_windows_forever_min, playtime_mac_forever_min,
        playtime_linux_forever_min, playtime_deck_forever_min,
        etl_run_id
    )
    ON target.user_key = source.user_key
   AND target.game_key = source.game_key
   AND target.date_key = source.date_key
    WHEN MATCHED THEN
        UPDATE SET
            playtime_2weeks_min          = source.playtime_2weeks_min,
            playtime_forever_min         = source.playtime_forever_min,
            playtime_windows_forever_min = source.playtime_windows_forever_min,
            playtime_mac_forever_min     = source.playtime_mac_forever_min,
            playtime_linux_forever_min   = source.playtime_linux_forever_min,
            playtime_deck_forever_min    = source.playtime_deck_forever_min,
            etl_run_id                   = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (
            user_key, game_key, date_key,
            playtime_2weeks_min, playtime_forever_min,
            playtime_windows_forever_min, playtime_mac_forever_min,
            playtime_linux_forever_min, playtime_deck_forever_min,
            etl_run_id
        )
        VALUES (
            source.user_key, source.game_key, source.date_key,
            source.playtime_2weeks_min, source.playtime_forever_min,
            source.playtime_windows_forever_min, source.playtime_mac_forever_min,
            source.playtime_linux_forever_min, source.playtime_deck_forever_min,
            source.etl_run_id
        );
""")


def get_recently_played(steamid: str, request_number: int = 0) -> list[dict]:
    api_key = _get_key(request_number)
    try:
        r = requests.get(
            RECENT_PLAY_URL,
            params={
                "key":     api_key,
                "steamid": steamid,
                "count":   0,  # 0 = todos los recientes
            },
            timeout=15,
        )
        if r.status_code in (401, 403):
            return []
        if r.status_code == 429:
            logger.warning(f"  429 steamid {steamid}, esperando 30s...")
            time.sleep(30)
            return get_recently_played(steamid, request_number + 1)
        r.raise_for_status()
        return r.json().get("response", {}).get("games", [])
    except requests.RequestException as e:
        logger.warning(f"  Error steamid {steamid}: {e}")
        return []


def load() -> None:
    run_id   = start_etl_run("load_fact_user_recent_play")
    inserted = 0
    skipped  = 0

    try:
        # Cargar mapas en memoria
        logger.info("Cargando mapas en memoria...")
        with get_session() as session:
            game_map = {
                row.appid: row.game_key
                for row in session.execute(SQL_GET_ALL_GAME_KEYS).fetchall()
            }
            users = session.execute(SQL_GET_USER).fetchall()

        logger.info(f"  {len(game_map):,} juegos | {len(users):,} usuarios")
        logger.info(f"  Keys activas: {len(_API_KEYS)}")

        # Necesitamos el steamid real para llamar a la API
        # Lo obtenemos del users_progress.json
        steamid_map = {}
        if USERS_PROGRESS.exists():
            with open(USERS_PROGRESS, "r", encoding="utf-8") as f:
                collected = json.load(f).get("collected", [])
            import hashlib
            for sid in collected:
                h = hashlib.sha256(sid.encode()).hexdigest()
                steamid_map[h] = sid
        
        # Debug: verificar que el mapa funciona
        found = sum(1 for u in users if u.steamid_hash in steamid_map)
        logger.info(f"  steamid_map tiene {len(steamid_map)} entradas")
        logger.info(f"  Usuarios de dim_user encontrados en steamid_map: {found}/{len(users)}")

        rows_batch  = []
        processed   = 0
        req_counter = 0

        for user_row in users:
            steamid = steamid_map.get(user_row.steamid_hash)
            if not steamid:
                skipped += 1
                continue

            games = get_recently_played(steamid, req_counter)
            req_counter += 1

            for game in games:
                appid    = game.get("appid")
                game_key = game_map.get(appid)
                if not appid or not game_key:
                    skipped += 1
                    continue

                rows_batch.append({
                    "user_key":                    user_row.user_key,
                    "game_key":                    game_key,
                    "date_key":                    TODAY_DATE_KEY,
                    "playtime_2weeks_min":         game.get("playtime_2weeks", 0),
                    "playtime_forever_min":        game.get("playtime_forever", 0),
                    "playtime_windows_forever_min": game.get("playtime_windows_forever", 0),
                    "playtime_mac_forever_min":    game.get("playtime_mac_forever", 0),
                    "playtime_linux_forever_min":  game.get("playtime_linux_forever", 0),
                    "playtime_deck_forever_min":   game.get("playtime_deck_forever", 0),
                    "etl_run_id":                  run_id,
                })

            processed += 1

            if len(rows_batch) >= 100:
                with get_session() as session:
                    session.execute(SQL_MERGE, rows_batch)
                inserted += len(rows_batch)
                rows_batch = []

            if processed % CHECKPOINT_EVERY == 0:
                logger.info(
                f"  [{processed:,}/{len(users):,} usuarios] "
                f"filas_insertadas={inserted:,} "
                f"filas_pendientes={len(rows_batch):,} "
                f"skip={skipped:,}"
            )

            time.sleep(DELAY_BETWEEN)

        if rows_batch:
            with get_session() as session:
                session.execute(SQL_MERGE, rows_batch)
            inserted += len(rows_batch)

        finish_etl_run(run_id, status="success",
                        rows_inserted=inserted, rows_skipped=skipped)
        logger.info(
            f"✅ fact_user_recent_play completado:"
            f"\n  Usuarios: {processed:,}"
            f"\n  Filas:    {inserted:,}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()