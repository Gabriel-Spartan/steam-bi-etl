# src/etl/load_dim_developer.py
import logging
from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run
from src.etl.steam_appdetails import get_appdetails_cached

logger = logging.getLogger(__name__)

SQL = text("""
    MERGE dim_developer AS target
    USING (VALUES (:developer_name, :etl_run_id))
        AS source (developer_name, etl_run_id)
    ON target.developer_name = source.developer_name
    WHEN MATCHED THEN
        UPDATE SET
            updated_at = GETDATE(),
            etl_run_id = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (developer_name, etl_run_id)
        VALUES (source.developer_name, source.etl_run_id);
""")


def extract_developers(all_details: list[dict], run_id: int) -> list[dict]:
    seen = set()
    developers = []

    for game in all_details:
        for name in game.get("developers", []):
            name = name.strip()
            if name and name not in seen:
                seen.add(name)
                developers.append({
                    "developer_name": name,
                    "etl_run_id":     run_id,
                })

    developers.sort(key=lambda x: x["developer_name"])
    logger.info(f"  {len(developers)} developers únicos extraídos.")
    return developers


def load() -> None:
    run_id = start_etl_run("load_dim_developer")
    try:
        all_details = get_appdetails_cached()
        developers = extract_developers(all_details, run_id)

        with get_session() as session:
            session.execute(SQL, developers)

        finish_etl_run(run_id, status="success", rows_inserted=len(developers))
        logger.info(f"dim_developer: {len(developers)} developers cargados.")

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()