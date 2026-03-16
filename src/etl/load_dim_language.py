# src/etl/load_dim_language.py
import logging
from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run

logger = logging.getLogger(__name__)

LANGUAGES = [
    {"iso_code": "ar",     "language_name": "Árabe",                  "native_name": "العربية",           "steam_api_name": "arabic"},
    {"iso_code": "bg",     "language_name": "Búlgaro",                "native_name": "Български",         "steam_api_name": "bulgarian"},
    {"iso_code": "cs",     "language_name": "Checo",                  "native_name": "Čeština",           "steam_api_name": "czech"},
    {"iso_code": "da",     "language_name": "Danés",                  "native_name": "Dansk",             "steam_api_name": "danish"},
    {"iso_code": "de",     "language_name": "Alemán",                 "native_name": "Deutsch",           "steam_api_name": "german"},
    {"iso_code": "el",     "language_name": "Griego",                 "native_name": "Ελληνικά",          "steam_api_name": "greek"},
    {"iso_code": "en",     "language_name": "Inglés",                 "native_name": "English",           "steam_api_name": "english"},
    {"iso_code": "es",     "language_name": "Español",                "native_name": "Español",           "steam_api_name": "spanish"},
    {"iso_code": "es-419", "language_name": "Español (Latinoamérica)","native_name": "Español (Latinoamérica)", "steam_api_name": "latam"},
    {"iso_code": "fi",     "language_name": "Finés",                  "native_name": "Suomi",             "steam_api_name": "finnish"},
    {"iso_code": "fr",     "language_name": "Francés",                "native_name": "Français",          "steam_api_name": "french"},
    {"iso_code": "hu",     "language_name": "Húngaro",                "native_name": "Magyar",            "steam_api_name": "hungarian"},
    {"iso_code": "id",     "language_name": "Indonesio",              "native_name": "Bahasa Indonesia",  "steam_api_name": "indonesian"},
    {"iso_code": "it",     "language_name": "Italiano",               "native_name": "Italiano",          "steam_api_name": "italian"},
    {"iso_code": "ja",     "language_name": "Japonés",                "native_name": "日本語",             "steam_api_name": "japanese"},
    {"iso_code": "ko",     "language_name": "Coreano",                "native_name": "한국어",             "steam_api_name": "koreana"},
    {"iso_code": "nl",     "language_name": "Neerlandés",             "native_name": "Nederlands",        "steam_api_name": "dutch"},
    {"iso_code": "no",     "language_name": "Noruego",                "native_name": "Norsk",             "steam_api_name": "norwegian"},
    {"iso_code": "pl",     "language_name": "Polaco",                 "native_name": "Polski",            "steam_api_name": "polish"},
    {"iso_code": "pt",     "language_name": "Portugués",              "native_name": "Português",         "steam_api_name": "portuguese"},
    {"iso_code": "pt-BR",  "language_name": "Portugués (Brasil)",     "native_name": "Português (Brasil)","steam_api_name": "brazilian"},
    {"iso_code": "ro",     "language_name": "Rumano",                 "native_name": "Română",            "steam_api_name": "romanian"},
    {"iso_code": "ru",     "language_name": "Ruso",                   "native_name": "Русский",           "steam_api_name": "russian"},
    {"iso_code": "sk",     "language_name": "Eslovaco",               "native_name": "Slovenčina",        "steam_api_name": "slovak"},
    {"iso_code": "sv",     "language_name": "Sueco",                  "native_name": "Svenska",           "steam_api_name": "swedish"},
    {"iso_code": "th",     "language_name": "Tailandés",              "native_name": "ไทย",               "steam_api_name": "thai"},
    {"iso_code": "tr",     "language_name": "Turco",                  "native_name": "Türkçe",            "steam_api_name": "turkish"},
    {"iso_code": "uk",     "language_name": "Ucraniano",              "native_name": "Українська",        "steam_api_name": "ukrainian"},
    {"iso_code": "vi",     "language_name": "Vietnamita",             "native_name": "Tiếng Việt",        "steam_api_name": "vietnamese"},
    {"iso_code": "zh-CN",  "language_name": "Chino simplificado",     "native_name": "简体中文",           "steam_api_name": "schinese"},
    {"iso_code": "zh-TW",  "language_name": "Chino tradicional",      "native_name": "繁體中文",           "steam_api_name": "tchinese"},
]

SQL = text("""
    MERGE dim_language AS target
    USING (VALUES (:iso_code, :language_name, :native_name, :steam_api_name, :etl_run_id))
        AS source (iso_code, language_name, native_name, steam_api_name, etl_run_id)
    ON target.iso_code = source.iso_code
    WHEN MATCHED THEN
        UPDATE SET
            language_name  = source.language_name,
            native_name    = source.native_name,
            steam_api_name = source.steam_api_name,
            updated_at     = GETDATE(),
            etl_run_id     = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (iso_code, language_name, native_name, steam_api_name, etl_run_id)
        VALUES (source.iso_code, source.language_name, source.native_name,
                source.steam_api_name, source.etl_run_id);
""")


def load() -> None:
    run_id = start_etl_run("load_dim_language")
    try:
        rows = [{**l, "etl_run_id": run_id} for l in LANGUAGES]

        with get_session() as session:
            session.execute(SQL, rows)

        finish_etl_run(run_id, status="success", rows_inserted=len(rows))
        logger.info(f"dim_language: {len(rows)} idiomas procesados.")
    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()