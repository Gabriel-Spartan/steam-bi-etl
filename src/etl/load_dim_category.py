# src/etl/load_dim_category.py
import logging
from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run
from src.etl.steam_appdetails import get_appdetails_cached

logger = logging.getLogger(__name__)

SQL = text("""
    MERGE dim_category AS target
    USING (VALUES (:category_id, :category_description, :etl_run_id))
        AS source (category_id, category_description, etl_run_id)
    ON target.category_id = source.category_id
    WHEN MATCHED THEN
        UPDATE SET
            category_description = source.category_description,
            updated_at           = GETDATE(),
            etl_run_id           = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (category_id, category_description, etl_run_id)
        VALUES (source.category_id, source.category_description, source.etl_run_id);
""")


def extract_categories(all_details: list[dict], run_id: int) -> list[dict]:
    seen = {}
    for game in all_details:
        for c in game.get("categories", []):
            cid = int(c.get("id", 0))
            if cid and cid not in seen:
                seen[cid] = {
                    "category_id":          cid,
                    "category_description": c.get("description", "").strip(),
                    "etl_run_id":           run_id,
                }
    categories = sorted(seen.values(), key=lambda x: x["category_id"])
    logger.info(f"  {len(categories)} categorías únicas extraídas.")
    return categories


def load() -> None:
    run_id = start_etl_run("load_dim_category")
    try:
        all_details = get_appdetails_cached()
        categories = extract_categories(all_details, run_id)

        with get_session() as session:
            session.execute(SQL, categories)

        finish_etl_run(run_id, status="success", rows_inserted=len(categories))
        logger.info(f"dim_category: {len(categories)} categorías cargadas.")

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()
