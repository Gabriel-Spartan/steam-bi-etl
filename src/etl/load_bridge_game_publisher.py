# src/etl/load_bridge_game_publisher.py
import logging
from sqlalchemy import text
from src.db import get_session, start_etl_run, finish_etl_run
from src.etl.steam_appdetails import get_appdetails_cached

logger = logging.getLogger(__name__)

SQL = text("""
    MERGE bridge_game_publisher AS target
    USING (VALUES (:game_key, :publisher_key))
        AS source (game_key, publisher_key)
    ON target.game_key = source.game_key
   AND target.publisher_key = source.publisher_key
    WHEN NOT MATCHED THEN
        INSERT (game_key, publisher_key)
        VALUES (source.game_key, source.publisher_key);
""")

SQL_GET_GAME_KEY      = text("SELECT game_key FROM dim_game WHERE appid = :appid AND is_current = 1")
SQL_GET_PUBLISHER_KEY = text("SELECT publisher_key FROM dim_publisher WHERE publisher_name = :publisher_name AND is_active = 1")


def load() -> None:
    run_id = start_etl_run("load_bridge_game_publisher")
    inserted = skipped = 0
    try:
        all_details = get_appdetails_cached()
        rows = []

        with get_session() as session:
            for game in all_details:
                appid = game.get("steam_appid")
                if not appid:
                    continue
                game_row = session.execute(SQL_GET_GAME_KEY, {"appid": appid}).fetchone()
                if not game_row:
                    skipped += 1
                    continue

                for name in game.get("publishers", []):
                    name = name.strip()
                    if not name:
                        continue
                    pub_row = session.execute(SQL_GET_PUBLISHER_KEY, {"publisher_name": name}).fetchone()
                    if not pub_row:
                        skipped += 1
                        continue
                    rows.append({
                        "game_key":      game_row.game_key,
                        "publisher_key": pub_row.publisher_key,
                    })

            if rows:
                session.execute(SQL, rows)
                inserted = len(rows)

        finish_etl_run(run_id, status="success", rows_inserted=inserted, rows_skipped=skipped)
        logger.info(f"✅ bridge_game_publisher: {inserted} relaciones cargadas.")
    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()