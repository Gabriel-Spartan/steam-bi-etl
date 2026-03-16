# src/etl/load_dim_date.py
import logging
from datetime import date, timedelta
from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run

logger = logging.getLogger(__name__)

DATE_START = date(2003, 9, 12)
DATE_END   = date(2030, 12, 31)

MONTHS_ES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

DAYS_ES = {
    0: "Lunes", 1: "Martes", 2: "Miércoles", 3: "Jueves",
    4: "Viernes", 5: "Sábado", 6: "Domingo"
}

SQL = text("""
    MERGE dim_date AS target
    USING (VALUES (
        :date_key, :full_date, :day_of_month, :month_number,
        :month_name, :quarter_number, :year_number,
        :day_of_week, :day_name, :etl_run_id
    )) AS source (
        date_key, full_date, day_of_month, month_number,
        month_name, quarter_number, year_number,
        day_of_week, day_name, etl_run_id
    )
    ON target.date_key = source.date_key
    WHEN NOT MATCHED THEN
        INSERT (date_key, full_date, day_of_month, month_number,
                month_name, quarter_number, year_number,
                day_of_week, day_name, etl_run_id)
        VALUES (source.date_key, source.full_date, source.day_of_month,
                source.month_number, source.month_name, source.quarter_number,
                source.year_number, source.day_of_week, source.day_name,
                source.etl_run_id);
""")


def generate_dates(run_id: int) -> list[dict]:
    rows = []
    current = DATE_START
    while current <= DATE_END:
        rows.append({
            "date_key":       int(current.strftime("%Y%m%d")),
            "full_date":      current,
            "day_of_month":   current.day,
            "month_number":   current.month,
            "month_name":     MONTHS_ES[current.month],
            "quarter_number": (current.month - 1) // 3 + 1,
            "year_number":    current.year,
            "day_of_week":    current.weekday() + 1,
            "day_name":       DAYS_ES[current.weekday()],
            "etl_run_id":     run_id,
        })
        current += timedelta(days=1)
    return rows


def load() -> None:
    run_id = start_etl_run("load_dim_date")
    try:
        logger.info(f"Generando fechas desde {DATE_START} hasta {DATE_END}...")
        rows = generate_dates(run_id)
        total = len(rows)
        batch_size = 1000
        inserted = 0

        with get_session() as session:
            for i in range(0, total, batch_size):
                batch = rows[i: i + batch_size]
                session.execute(SQL, batch)
                inserted += len(batch)
                logger.info(f"  {inserted:,} / {total:,} filas procesadas...")

        finish_etl_run(run_id, status="success", rows_inserted=total)
        logger.info(f"dim_date: {total:,} filas cargadas.")
    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()