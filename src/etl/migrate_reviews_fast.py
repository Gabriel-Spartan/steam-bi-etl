# src/etl/migrate_reviews_fast.py
import logging
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from sqlalchemy import text
from src.db import get_session

logger = logging.getLogger(__name__)

MONGO_URI  = "mongodb://localhost:27017"
MONGO_DB   = "steam_bi"
BATCH_SIZE = 1000  # reducido de 5000 a 1000


def migrate_reviews_fast():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")

    client = MongoClient(MONGO_URI)
    col    = client[MONGO_DB]["review_details"]
    col.drop()
    logger.info("Colección review_details limpiada.")

    # Cargar mapas en sesiones separadas y cerrarlas inmediatamente
    logger.info("Cargando mapas de referencia...")
    with get_session() as session:
        game_rows = session.execute(text("""
            SELECT game_key, appid, game_name
            FROM dim_game WHERE is_current = 1
        """)).fetchall()
        game_map = {r.game_key: (r.appid, r.game_name) for r in game_rows}

    with get_session() as session:
        lang_rows = session.execute(text("""
            SELECT language_key, steam_api_name FROM dim_language
        """)).fetchall()
        lang_map = {r.language_key: r.steam_api_name for r in lang_rows}

    with get_session() as session:
        total = session.execute(
            text("SELECT COUNT(*) as n FROM fact_game_review_detail")
        ).fetchone().n

    logger.info(f"  {total:,} reseñas | game_map: {len(game_map):,} | lang_map: {len(lang_map):,}")

    last_id  = ""
    inserted = 0

    while True:
        # Cada batch abre y cierra su propia sesión
        with get_session() as session:
            rows = session.execute(text("""
                SELECT TOP 1000
                    recommendation_id, game_key, language_key,
                    voted_up, votes_up, votes_funny,
                    weighted_vote_score, comment_count,
                    steam_purchase, received_for_free,
                    refunded, written_during_early_access,
                    primarily_steam_deck,
                    created_date_key, updated_date_key,
                    review_text,
                    author_playtime_forever,
                    author_playtime_last_two_weeks,
                    author_playtime_at_review,
                    author_last_played
                FROM fact_game_review_detail
                WHERE recommendation_id > :last_id
                ORDER BY recommendation_id ASC
            """), {"last_id": last_id}).fetchall()

        if not rows:
            break

        docs = []
        for r in rows:
            appid, game_name = game_map.get(r.game_key, (None, None))
            docs.append({
                "_id":             r.recommendation_id,
                "recommendation_id": r.recommendation_id,
                "appid":           appid,
                "game_name":       game_name,
                "language":        lang_map.get(r.language_key),
                "voted_up":        bool(r.voted_up),
                "votes_up":        r.votes_up,
                "votes_funny":     r.votes_funny,
                "weighted_vote_score": float(r.weighted_vote_score or 0),
                "comment_count":   r.comment_count,
                "steam_purchase":  bool(r.steam_purchase),
                "received_for_free": bool(r.received_for_free),
                "refunded":        bool(r.refunded),
                "written_during_early_access": bool(r.written_during_early_access),
                "primarily_steam_deck": bool(r.primarily_steam_deck),
                "created_date_key": r.created_date_key,
                "updated_date_key": r.updated_date_key,
                "review_text":     r.review_text,
                "author": {
                    "playtime_forever_min":   r.author_playtime_forever,
                    "playtime_2weeks_min":     r.author_playtime_last_two_weeks,
                    "playtime_at_review_min": r.author_playtime_at_review,
                    "last_played":            r.author_last_played.isoformat() if r.author_last_played else None,
                }
            })

        try:
            col.insert_many(docs, ordered=False)
        except BulkWriteError:
            pass

        # Liberar memoria explícitamente
        del docs
        del rows

        inserted += 1000
        last_id   = r.recommendation_id

        if inserted % 50000 == 0:
            logger.info(f"  [{inserted:,}/{total:,}] reseñas migradas")

    col.create_index("appid")
    col.create_index("language")
    col.create_index("voted_up")
    col.create_index([("weighted_vote_score", -1)])

    logger.info(f"✅ review_details: {inserted:,} documentos insertados.")
    client.close()


if __name__ == "__main__":
    migrate_reviews_fast()