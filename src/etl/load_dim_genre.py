# src/etl/load_dim_genre.py
import logging
from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run
from src.etl.steam_appdetails import get_appdetails_cached  # ← cambia el import

logger = logging.getLogger(__name__)

SQL = text("""
    MERGE dim_genre AS target
    USING (VALUES (:genre_id, :genre_description, :etl_run_id))
        AS source (genre_id, genre_description, etl_run_id)
    ON target.genre_id = source.genre_id
    WHEN MATCHED THEN
        UPDATE SET
            genre_description = source.genre_description,
            updated_at        = GETDATE(),
            etl_run_id        = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (genre_id, genre_description, etl_run_id)
        VALUES (source.genre_id, source.genre_description, source.etl_run_id);
""")


def extract_genres(all_details: list[dict], run_id: int) -> list[dict]:
    seen = {}
    for game in all_details:
        for g in game.get("genres", []):
            gid = int(g.get("id", 0))
            if gid and gid not in seen:
                seen[gid] = {
                    "genre_id":          gid,
                    "genre_description": g.get("description", "").strip(),
                    "etl_run_id":        run_id,
                }
    genres = sorted(seen.values(), key=lambda x: x["genre_id"])
    logger.info(f"  {len(genres)} géneros únicos extraídos.")
    return genres


def load() -> None:
    run_id = start_etl_run("load_dim_genre")
    try:
        all_details = get_appdetails_cached()  # ← usa caché automáticamente
        genres = extract_genres(all_details, run_id)

        with get_session() as session:
            session.execute(SQL, genres)

        finish_etl_run(run_id, status="success", rows_inserted=len(genres))
        logger.info(f"dim_genre: {len(genres)} géneros cargados.")

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()