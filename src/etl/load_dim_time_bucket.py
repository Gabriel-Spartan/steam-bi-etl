# src/etl/load_dim_time_bucket.py
import logging
from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run

logger = logging.getLogger(__name__)

TIME_BUCKETS = [
    {"bucket_name": "madrugada"},
    {"bucket_name": "mañana"},
    {"bucket_name": "tarde"},
    {"bucket_name": "noche"},
]

SQL = text("""
    MERGE dim_time_bucket AS target
    USING (VALUES (:bucket_name, :etl_run_id)) AS source (bucket_name, etl_run_id)
    ON target.bucket_name = source.bucket_name
    WHEN MATCHED THEN
        UPDATE SET
            updated_at = GETDATE(),
            etl_run_id = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (bucket_name, etl_run_id)
        VALUES (source.bucket_name, source.etl_run_id);
""")


def load() -> None:
    run_id = start_etl_run("load_dim_time_bucket")
    try:
        rows = [{**b, "etl_run_id": run_id} for b in TIME_BUCKETS]

        with get_session() as session:
            session.execute(SQL, rows)

        finish_etl_run(run_id, status="success", rows_inserted=len(rows))
        logger.info(f"dim_time_bucket: {len(rows)} registros procesados.")
    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()