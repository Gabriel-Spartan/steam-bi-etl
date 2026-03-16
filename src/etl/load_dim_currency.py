# src/etl/load_dim_currency.py
import logging
from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run

logger = logging.getLogger(__name__)

CURRENCIES = [
    {"currency_code": "USD", "currency_name": "Dólar estadounidense",  "currency_symbol": "$",    "minor_unit": 2},
    {"currency_code": "EUR", "currency_name": "Euro",                  "currency_symbol": "€",    "minor_unit": 2},
    {"currency_code": "GBP", "currency_name": "Libra esterlina",       "currency_symbol": "£",    "minor_unit": 2},
    {"currency_code": "BRL", "currency_name": "Real brasileño",        "currency_symbol": "R$",   "minor_unit": 2},
    {"currency_code": "RUB", "currency_name": "Rublo ruso",            "currency_symbol": "₽",    "minor_unit": 2},
    {"currency_code": "JPY", "currency_name": "Yen japonés",           "currency_symbol": "¥",    "minor_unit": 0},
    {"currency_code": "CNY", "currency_name": "Yuan chino",            "currency_symbol": "¥",    "minor_unit": 2},
    {"currency_code": "KRW", "currency_name": "Won surcoreano",        "currency_symbol": "₩",    "minor_unit": 0},
    {"currency_code": "CAD", "currency_name": "Dólar canadiense",      "currency_symbol": "C$",   "minor_unit": 2},
    {"currency_code": "AUD", "currency_name": "Dólar australiano",     "currency_symbol": "A$",   "minor_unit": 2},
    {"currency_code": "HKD", "currency_name": "Dólar de Hong Kong",    "currency_symbol": "HK$",  "minor_unit": 2},
    {"currency_code": "INR", "currency_name": "Rupia india",           "currency_symbol": "₹",    "minor_unit": 2},
    {"currency_code": "MXN", "currency_name": "Peso mexicano",         "currency_symbol": "$",    "minor_unit": 2},
    {"currency_code": "ARS", "currency_name": "Peso argentino",        "currency_symbol": "$",    "minor_unit": 2},
    {"currency_code": "CLP", "currency_name": "Peso chileno",          "currency_symbol": "$",    "minor_unit": 0},
    {"currency_code": "COP", "currency_name": "Peso colombiano",       "currency_symbol": "$",    "minor_unit": 2},
    {"currency_code": "PEN", "currency_name": "Sol peruano",           "currency_symbol": "S/",   "minor_unit": 2},
    {"currency_code": "UYU", "currency_name": "Peso uruguayo",         "currency_symbol": "$",    "minor_unit": 2},
    {"currency_code": "NZD", "currency_name": "Dólar neozelandés",     "currency_symbol": "NZ$",  "minor_unit": 2},
    {"currency_code": "SGD", "currency_name": "Dólar de Singapur",     "currency_symbol": "S$",   "minor_unit": 2},
    {"currency_code": "MYR", "currency_name": "Ringgit malayo",        "currency_symbol": "RM",   "minor_unit": 2},
    {"currency_code": "PHP", "currency_name": "Peso filipino",         "currency_symbol": "₱",    "minor_unit": 2},
    {"currency_code": "THB", "currency_name": "Baht tailandés",        "currency_symbol": "฿",    "minor_unit": 2},
    {"currency_code": "IDR", "currency_name": "Rupia indonesia",       "currency_symbol": "Rp",   "minor_unit": 2},
    {"currency_code": "VND", "currency_name": "Dong vietnamita",       "currency_symbol": "₫",    "minor_unit": 0},
    {"currency_code": "TWD", "currency_name": "Nuevo dólar taiwanés",  "currency_symbol": "NT$",  "minor_unit": 2},
    {"currency_code": "SAR", "currency_name": "Riyal saudí",           "currency_symbol": "﷼",    "minor_unit": 2},
    {"currency_code": "AED", "currency_name": "Dírham emiratí",        "currency_symbol": "د.إ",  "minor_unit": 2},
    {"currency_code": "ILS", "currency_name": "Séquel israelí",        "currency_symbol": "₪",    "minor_unit": 2},
    {"currency_code": "TRY", "currency_name": "Lira turca",            "currency_symbol": "₺",    "minor_unit": 2},
    {"currency_code": "NOK", "currency_name": "Corona noruega",        "currency_symbol": "kr",   "minor_unit": 2},
    {"currency_code": "SEK", "currency_name": "Corona sueca",          "currency_symbol": "kr",   "minor_unit": 2},
    {"currency_code": "DKK", "currency_name": "Corona danesa",         "currency_symbol": "kr",   "minor_unit": 2},
    {"currency_code": "CHF", "currency_name": "Franco suizo",          "currency_symbol": "Fr",   "minor_unit": 2},
    {"currency_code": "PLN", "currency_name": "Esloti polaco",         "currency_symbol": "zł",   "minor_unit": 2},
    {"currency_code": "CZK", "currency_name": "Corona checa",          "currency_symbol": "Kč",   "minor_unit": 2},
    {"currency_code": "HUF", "currency_name": "Forinto húngaro",       "currency_symbol": "Ft",   "minor_unit": 2},
    {"currency_code": "RON", "currency_name": "Leu rumano",            "currency_symbol": "lei",  "minor_unit": 2},
    {"currency_code": "UAH", "currency_name": "Grivna ucraniana",      "currency_symbol": "₴",    "minor_unit": 2},
    {"currency_code": "KZT", "currency_name": "Tenge kazajo",          "currency_symbol": "₸",    "minor_unit": 2},
    {"currency_code": "ZAR", "currency_name": "Rand sudafricano",      "currency_symbol": "R",    "minor_unit": 2},
    {"currency_code": "CRC", "currency_name": "Colón costarricense",   "currency_symbol": "₡",    "minor_unit": 2},
    {"currency_code": "KWD", "currency_name": "Dinar kuwaití",         "currency_symbol": "د.ك",  "minor_unit": 3},
    {"currency_code": "QAR", "currency_name": "Riyal catarí",          "currency_symbol": "﷼",    "minor_unit": 2},
]

SQL = text("""
    MERGE dim_currency AS target
    USING (VALUES (:currency_code, :currency_name, :currency_symbol, :minor_unit, :etl_run_id))
        AS source (currency_code, currency_name, currency_symbol, minor_unit, etl_run_id)
    ON target.currency_code = source.currency_code
    WHEN MATCHED THEN
        UPDATE SET
            currency_name   = source.currency_name,
            currency_symbol = source.currency_symbol,
            minor_unit      = source.minor_unit,
            updated_at      = GETDATE(),
            etl_run_id      = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (currency_code, currency_name, currency_symbol, minor_unit, etl_run_id)
        VALUES (source.currency_code, source.currency_name, source.currency_symbol,
                source.minor_unit, source.etl_run_id);
""")


def load() -> None:
    run_id = start_etl_run("load_dim_currency")
    try:
        rows = [{**c, "etl_run_id": run_id} for c in CURRENCIES]

        with get_session() as session:
            session.execute(SQL, rows)

        finish_etl_run(run_id, status="success", rows_inserted=len(rows))
        logger.info(f"dim_currency: {len(rows)} monedas procesadas.")
    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()