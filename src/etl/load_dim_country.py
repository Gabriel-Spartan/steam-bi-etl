# src/etl/load_dim_country.py
import logging
import requests
from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run

logger = logging.getLogger(__name__)

RESTCOUNTRIES_URL = "https://restcountries.com/v3.1/all?fields=cca2,name,translations"


def fetch_countries() -> list[dict]:
    logger.info("Consultando RestCountries API...")
    response = requests.get(RESTCOUNTRIES_URL, timeout=15)
    response.raise_for_status()

    countries = []
    for item in response.json():
        iso_code = item.get("cca2", "").strip()
        translations = item.get("translations", {})
        country_name = translations.get("spa", {}).get("common", "").strip()
        if not country_name:
            country_name = item.get("name", {}).get("common", "").strip()
        if not iso_code or not country_name:
            continue
        countries.append({"iso_code": iso_code, "country_name": country_name})

    countries.sort(key=lambda x: x["country_name"])
    logger.info(f"  {len(countries)} países obtenidos.")
    return countries


SQL = text("""
    MERGE dim_country AS target
    USING (VALUES (:iso_code, :country_name, :etl_run_id))
        AS source (iso_code, country_name, etl_run_id)
    ON target.iso_code = source.iso_code
    WHEN MATCHED THEN
        UPDATE SET
            country_name = source.country_name,
            updated_at   = GETDATE(),
            etl_run_id   = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (iso_code, country_name, etl_run_id)
        VALUES (source.iso_code, source.country_name, source.etl_run_id);
""")


def load() -> None:
    run_id = start_etl_run("load_dim_country")
    try:
        countries = fetch_countries()
        rows = [{**c, "etl_run_id": run_id} for c in countries]

        with get_session() as session:
            session.execute(SQL, rows)

        finish_etl_run(run_id, status="success", rows_inserted=len(rows))
        logger.info(f"dim_country: {len(rows)} países procesados.")
    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()