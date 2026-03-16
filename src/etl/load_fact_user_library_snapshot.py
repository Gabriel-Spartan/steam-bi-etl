# src/etl/load_fact_user_library_snapshot.py
"""
Carga fact_user_library_snapshot desde user_libraries.jsonl.
Granularidad: 1 fila por user_key + date_key.
"""
import json
import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run

logger = logging.getLogger(__name__)

LIBRARIES_CACHE  = Path(__file__).resolve().parents[2] / "data" / "cache" / "user_libraries.jsonl"
TODAY_DATE_KEY   = int(datetime.now().strftime("%Y%m%d"))
CHECKPOINT_EVERY = 1000

SQL_GET_ALL_USER_KEYS = text("""
    SELECT steamid_hash, user_key FROM dim_user WHERE is_current = 1
""")

SQL_MERGE = text("""
    MERGE fact_user_library_snapshot AS target
    USING (VALUES (:user_key, :date_key, :game_count, :etl_run_id))
        AS source (user_key, date_key, game_count, etl_run_id)
    ON target.user_key = source.user_key
   AND target.date_key = source.date_key
    WHEN MATCHED THEN
        UPDATE SET
            game_count = source.game_count,
            etl_run_id = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (user_key, date_key, game_count, etl_run_id)
        VALUES (source.user_key, source.date_key, source.game_count, source.etl_run_id);
""")


def load() -> None:
    run_id   = start_etl_run("load_fact_user_library_snapshot")
    inserted = 0
    skipped  = 0

    try:
        if not LIBRARIES_CACHE.exists():
            logger.error("No se encontró user_libraries.jsonl.")
            finish_etl_run(run_id, status="failed",
                           error_message="user_libraries.jsonl no existe")
            return

        logger.info("Cargando user_map en memoria...")
        with get_session() as session:
            user_map = {
                row.steamid_hash: row.user_key
                for row in session.execute(SQL_GET_ALL_USER_KEYS).fetchall()
            }
        logger.info(f"  {len(user_map):,} usuarios cargados.")

        rows      = []
        processed = 0

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
                    user_key = user_map.get(steam_hash)
                    if not user_key:
                        skipped += 1
                        continue

                    game_count = len(user_data.get("games", []))
                    rows.append({
                        "user_key":   user_key,
                        "date_key":   TODAY_DATE_KEY,
                        "game_count": game_count,
                        "etl_run_id": run_id,
                    })
                    processed += 1

                    if len(rows) >= 1000:
                        with get_session() as session:
                            session.execute(SQL_MERGE, rows)
                        inserted += len(rows)
                        rows = []

                        if processed % CHECKPOINT_EVERY == 0:
                            logger.info(f"  [{processed:,} usuarios] filas={inserted:,}")

        if rows:
            with get_session() as session:
                session.execute(SQL_MERGE, rows)
            inserted += len(rows)

        finish_etl_run(run_id, status="success", rows_inserted=inserted, rows_skipped=skipped)
        logger.info(
            f"✅ fact_user_library_snapshot completado:"
            f"\n  Usuarios: {processed:,}"
            f"\n  Filas:    {inserted:,}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()