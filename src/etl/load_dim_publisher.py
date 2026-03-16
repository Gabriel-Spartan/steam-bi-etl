# src/etl/load_dim_publisher.py
import logging
from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run
from src.etl.steam_appdetails import get_appdetails_cached

logger = logging.getLogger(__name__)

SQL = text("""
    MERGE dim_publisher AS target
    USING (VALUES (:publisher_name, :etl_run_id))
        AS source (publisher_name, etl_run_id)
    ON target.publisher_name = source.publisher_name
    WHEN MATCHED THEN
        UPDATE SET
            updated_at = GETDATE(),
            etl_run_id = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (publisher_name, etl_run_id)
        VALUES (source.publisher_name, source.etl_run_id);
""")


def extract_publishers(all_details: list[dict], run_id: int) -> list[dict]:
    seen = set()
    publishers = []

    for game in all_details:
        for name in game.get("publishers", []):
            name = name.strip()
            if name and name not in seen:
                seen.add(name)
                publishers.append({
                    "publisher_name": name,
                    "etl_run_id":     run_id,
                })

    publishers.sort(key=lambda x: x["publisher_name"])
    logger.info(f"  {len(publishers)} publishers únicos extraídos.")
    return publishers


def load() -> None:
    run_id = start_etl_run("load_dim_publisher")
    try:
        all_details = get_appdetails_cached()
        publishers = extract_publishers(all_details, run_id)

        with get_session() as session:
            session.execute(SQL, publishers)

        finish_etl_run(run_id, status="success", rows_inserted=len(publishers))
        logger.info(f"dim_publisher: {len(publishers)} publishers cargados.")

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()