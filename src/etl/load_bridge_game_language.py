# src/etl/load_bridge_game_language.py
import logging
import re
from sqlalchemy import text
from src.db import get_session, start_etl_run, finish_etl_run
from src.etl.steam_appdetails import get_appdetails_cached

logger = logging.getLogger(__name__)

SQL = text("""
    MERGE bridge_game_language AS target
    USING (VALUES (:game_key, :language_key, :has_interface, :has_audio, :has_subtitles))
        AS source (game_key, language_key, has_interface, has_audio, has_subtitles)
    ON target.game_key = source.game_key
   AND target.language_key = source.language_key
    WHEN MATCHED THEN
        UPDATE SET
            has_interface = source.has_interface,
            has_audio     = source.has_audio,
            has_subtitles = source.has_subtitles
    WHEN NOT MATCHED THEN
        INSERT (game_key, language_key, has_interface, has_audio, has_subtitles)
        VALUES (source.game_key, source.language_key,
                source.has_interface, source.has_audio, source.has_subtitles);
""")

SQL_GET_GAME_KEY     = text("SELECT game_key FROM dim_game WHERE appid = :appid AND is_current = 1")
SQL_GET_LANGUAGE_MAP = text("SELECT language_key, steam_api_name, iso_code FROM dim_language")

# Mapa de nombres que Steam usa en supported_languages al nombre de steam_api_name
STEAM_LANGUAGE_NAME_MAP = {
    # Español (como los devuelve la API con l=spanish)
    "inglés":                       "english",
    "alemán":                       "german",
    "francés":                      "french",
    "español de españa":            "spanish",
    "español de hispanoamérica":    "latam",
    "portugués de brasil":          "brazilian",
    "portugués de portugal":        "portuguese",
    "ruso":                         "russian",
    "japonés":                      "japanese",
    "coreano":                      "koreana",
    "chino simplificado":           "schinese",
    "chino tradicional":            "tchinese",
    "italiano":                     "italian",
    "holandés":                     "dutch",
    "polaco":                       "polish",
    "turco":                        "turkish",
    "árabe":                        "arabic",
    "tailandés":                    "thai",
    "vietnamita":                   "vietnamese",
    "checo":                        "czech",
    "húngaro":                      "hungarian",
    "rumano":                       "romanian",
    "ucraniano":                    "ukrainian",
    "búlgaro":                      "bulgarian",
    "griego":                       "greek",
    "danés":                        "danish",
    "finés":                        "finnish",
    "noruego":                      "norwegian",
    "sueco":                        "swedish",
    "eslovaco":                     "slovak",
    "indonesio":                    "indonesian",
}


def parse_supported_languages(raw: str) -> list[dict]:
    """
    Parsea el campo supported_languages de appdetails.
    Formato: 'English, French, German<strong>*</strong>, Spanish...'
    <strong>*</strong> indica que tiene audio.
    Devuelve lista de dicts con {name, has_interface, has_audio, has_subtitles}
    """
    if not raw:
        return []

    results = []
    # Separar por coma, limpiar HTML
    parts = raw.split(",")

    for part in parts:
        has_audio     = "<strong>" in part.lower() or "*" in part
        has_subtitles = False  # Steam no distingue subtítulos en este campo

        # Limpiar HTML tags
        clean = re.sub(r"<[^>]+>", "", part).strip().lower()
        clean = clean.replace("*", "").strip()

        if not clean:
            continue

        results.append({
            "name":          clean,
            "has_interface": True,   # si aparece en la lista, tiene interfaz
            "has_audio":     has_audio,
            "has_subtitles": has_subtitles,
        })

    return results


def load() -> None:
    run_id = start_etl_run("load_bridge_game_language")
    inserted = skipped = 0
    try:
        all_details = get_appdetails_cached()

        # Construir mapa language: steam_api_name → language_key
        with get_session() as session:
            lang_rows = session.execute(SQL_GET_LANGUAGE_MAP).fetchall()

        api_name_to_key = {
            row.steam_api_name: row.language_key
            for row in lang_rows
            if row.steam_api_name
        }

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

                raw_languages = game.get("supported_languages", "")
                parsed = parse_supported_languages(raw_languages)

                for lang in parsed:
                    # Mapear nombre de pantalla → steam_api_name → language_key
                    api_name = STEAM_LANGUAGE_NAME_MAP.get(lang["name"])
                    if not api_name:
                        skipped += 1
                        continue

                    lang_key = api_name_to_key.get(api_name)
                    if not lang_key:
                        skipped += 1
                        continue

                    rows.append({
                        "game_key":      game_row.game_key,
                        "language_key":  lang_key,
                        "has_interface": 1 if lang["has_interface"] else 0,
                        "has_audio":     1 if lang["has_audio"] else 0,
                        "has_subtitles": 1 if lang["has_subtitles"] else 0,
                    })

            if rows:
                session.execute(SQL, rows)
                inserted = len(rows)

        finish_etl_run(run_id, status="success", rows_inserted=inserted, rows_skipped=skipped)
        logger.info(f"✅ bridge_game_language: {inserted} relaciones cargadas.")
    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()