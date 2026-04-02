# src/etl/migrate_to_mongodb.py
"""
Migración de Steam_BI (SQL Server) a MongoDB.
Genera 3 colecciones:
  - games:          ~50,000 documentos (juego + todo embebido)
  - users:          ~8,534  documentos (usuario + actividad)
  - review_details: ~2.7M   documentos (reseñas individuales)

Uso:
  python -m src.etl.migrate_to_mongodb
  python -m src.etl.migrate_to_mongodb --collection games
  python -m src.etl.migrate_to_mongodb --drop  # limpia antes de migrar
"""
import argparse
import logging
from datetime import datetime

from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError
from sqlalchemy import text

from src.db import get_session

logger = logging.getLogger(__name__)

MONGO_URI  = "mongodb://localhost:27017"
MONGO_DB   = "steam_bi"
BATCH_SIZE = 500


# ── Conexión MongoDB ──────────────────────────────────────────────────────────

def get_mongo_db():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]


# ── COLECCIÓN: games ──────────────────────────────────────────────────────────

def migrate_games(db, drop: bool = False) -> None:
    col = db["games"]
    if drop:
        col.drop()
        logger.info("Colección 'games' eliminada.")

    logger.info("Migrando colección 'games'...")

    with get_session() as session:
        # Obtener todos los juegos vigentes
        games = session.execute(text("""
            SELECT
                g.game_key, g.appid, g.game_name, g.game_type,
                g.required_age, g.is_free, g.controller_support,
                g.website, g.release_date, g.coming_soon,
                g.recommendations_total, g.achievements_total,
                g.metacritic_score, g.platform_windows,
                g.platform_mac, g.platform_linux
            FROM dim_game g
            WHERE g.is_current = 1 AND g.is_active = 1
        """)).fetchall()

        total = len(games)
        logger.info(f"  {total:,} juegos a migrar.")

        # Cargar lookups en memoria
        genres = session.execute(text("""
            SELECT bg.game_key, g.genre_description as genre_name
            FROM bridge_game_genre bg
            JOIN dim_genre g ON g.genre_key = bg.genre_key
        """)).fetchall()
        genre_map = {}
        for r in genres:
            genre_map.setdefault(r.game_key, []).append(r.genre_name)

        categories = session.execute(text("""
            SELECT bc.game_key, c.category_description as category_name
            FROM bridge_game_category bc
            JOIN dim_category c ON c.category_key = bc.category_key
        """)).fetchall()
        category_map = {}
        for r in categories:
            category_map.setdefault(r.game_key, []).append(r.category_name)

        developers = session.execute(text("""
            SELECT bd.game_key, d.developer_name
            FROM bridge_game_developer bd
            JOIN dim_developer d ON d.developer_key = bd.developer_key
        """)).fetchall()
        developer_map = {}
        for r in developers:
            developer_map.setdefault(r.game_key, []).append(r.developer_name)

        publishers = session.execute(text("""
            SELECT bp.game_key, p.publisher_name
            FROM bridge_game_publisher bp
            JOIN dim_publisher p ON p.publisher_key = bp.publisher_key
        """)).fetchall()
        publisher_map = {}
        for r in publishers:
            publisher_map.setdefault(r.game_key, []).append(r.publisher_name)

        languages = session.execute(text("""
            SELECT bl.game_key, l.language_name, l.steam_api_name,
                   bl.has_interface, bl.has_audio, bl.has_subtitles
            FROM bridge_game_language bl
            JOIN dim_language l ON l.language_key = bl.language_key
        """)).fetchall()
        language_map = {}
        for r in languages:
            language_map.setdefault(r.game_key, []).append({
                "language":      r.language_name,
                "steam_api_name": r.steam_api_name,
                "has_interface": bool(r.has_interface),
                "has_audio":     bool(r.has_audio),
                "has_subtitles": bool(r.has_subtitles),
            })

        price_snapshots = session.execute(text("""
            SELECT f.game_key, f.date_key, f.captured_at,
                   f.initial_price, f.final_price, f.discount_percent,
                   c.iso_code as country, cur.currency_code as currency
            FROM fact_game_price_snapshot f
            JOIN dim_country c ON c.country_key = f.country_key
            JOIN dim_currency cur ON cur.currency_key = f.currency_key
            ORDER BY f.game_key, f.captured_at
        """)).fetchall()
        price_map = {}
        for r in price_snapshots:
            price_map.setdefault(r.game_key, []).append({
                "date_key":        r.date_key,
                "captured_at":     r.captured_at.isoformat() if r.captured_at else None,
                "country":         r.country,
                "currency":        r.currency,
                "initial_price":   r.initial_price,
                "final_price":     r.final_price,
                "discount_percent": r.discount_percent,
            })

        review_summaries = session.execute(text("""
            SELECT game_key, date_key, review_score,
                   review_score_desc, total_positive,
                   total_negative, total_reviews
            FROM fact_game_review_summary
        """)).fetchall()
        review_summary_map = {}
        for r in review_summaries:
            review_summary_map[r.game_key] = {
                "date_key":         r.date_key,
                "review_score":     r.review_score,
                "review_score_desc": r.review_score_desc,
                "total_positive":   r.total_positive,
                "total_negative":   r.total_negative,
                "total_reviews":    r.total_reviews,
            }

        ccu_snapshots = session.execute(text("""
            SELECT game_key, captured_at, date_key, current_player_count
            FROM fact_concurrent_players_snapshot
            ORDER BY game_key, captured_at
        """)).fetchall()
        ccu_map = {}
        for r in ccu_snapshots:
            ccu_map.setdefault(r.game_key, []).append({
                "date_key":      r.date_key,
                "captured_at":   r.captured_at.isoformat() if r.captured_at else None,
                "player_count":  r.current_player_count,
            })

        news = session.execute(text("""
            SELECT game_key, news_gid, title, date_published,
                   date_key, feed_label, url, author, contents_short
            FROM fact_news_events
            ORDER BY game_key, date_published DESC
        """)).fetchall()
        news_map = {}
        for r in news:
            if len(news_map.get(r.game_key, [])) < 10:
                news_map.setdefault(r.game_key, []).append({
                    "news_gid":       r.news_gid,
                    "title":          r.title,
                    "date_published": r.date_published.isoformat() if r.date_published else None,
                    "date_key":       r.date_key,
                    "feed_label":     r.feed_label,
                    "url":            r.url,
                    "author":         r.author,
                    "contents_short": r.contents_short,
                })

        ach_summaries = session.execute(text("""
            SELECT game_key, date_key, achievement_count_total,
                   most_common_achievement_name, most_common_percent,
                   rarest_achievement_name, rarest_percent,
                   closest_25_name, closest_25_percent,
                   closest_50_name, closest_50_percent,
                   closest_75_name, closest_75_percent,
                   share_under_5_percent, share_5_to_25_percent,
                   share_25_to_50_percent, share_50_to_75_percent,
                   share_over_75_percent
            FROM fact_game_achievement_summary
        """)).fetchall()
        ach_summary_map = {}
        for r in ach_summaries:
            ach_summary_map[r.game_key] = {
                "date_key":              r.date_key,
                "total":                 r.achievement_count_total,
                "most_common":           {"name": r.most_common_achievement_name, "percent": float(r.most_common_percent or 0)},
                "rarest":                {"name": r.rarest_achievement_name, "percent": float(r.rarest_percent or 0)},
                "closest_25":            {"name": r.closest_25_name, "percent": float(r.closest_25_percent or 0)},
                "closest_50":            {"name": r.closest_50_name, "percent": float(r.closest_50_percent or 0)},
                "closest_75":            {"name": r.closest_75_name, "percent": float(r.closest_75_percent or 0)},
                "distribution": {
                    "under_5_pct":   float(r.share_under_5_percent or 0),
                    "5_to_25_pct":   float(r.share_5_to_25_percent or 0),
                    "25_to_50_pct":  float(r.share_25_to_50_percent or 0),
                    "50_to_75_pct":  float(r.share_50_to_75_percent or 0),
                    "over_75_pct":   float(r.share_over_75_percent or 0),
                }
            }

        ach_global = session.execute(text("""
            SELECT f.game_key, a.achievement_api_name,
                   a.achievement_display_name, a.is_hidden,
                   f.global_unlock_percent
            FROM fact_achievement_global f
            JOIN dim_achievement a ON a.achievement_key = f.achievement_key
            WHERE f.date_key = (SELECT MAX(date_key) FROM fact_achievement_global)
              AND a.is_active = 1
            ORDER BY f.game_key, f.global_unlock_percent DESC
        """)).fetchall()
        ach_global_map = {}
        for r in ach_global:
            ach_global_map.setdefault(r.game_key, []).append({
                "api_name":    r.achievement_api_name,
                "display_name": r.achievement_display_name,
                "is_hidden":   bool(r.is_hidden),
                "percent":     float(r.global_unlock_percent),
            })

    # Construir e insertar documentos en lotes
    batch = []
    inserted = 0

    for i, g in enumerate(games, 1):
        doc = {
            "_id":                  g.appid,
            "appid":                g.appid,
            "game_key":             g.game_key,
            "game_name":            g.game_name,
            "game_type":            g.game_type,
            "required_age":         g.required_age,
            "is_free":              bool(g.is_free),
            "controller_support":   g.controller_support,
            "website":              g.website,
            "release_date":         str(g.release_date) if g.release_date else None,
            "coming_soon":          bool(g.coming_soon) if g.coming_soon is not None else False,
            "recommendations_total": g.recommendations_total,
            "achievements_total":   g.achievements_total,
            "metacritic_score":     g.metacritic_score,
            "platforms": {
                "windows": bool(g.platform_windows),
                "mac":     bool(g.platform_mac),
                "linux":   bool(g.platform_linux),
            },
            "genres":      genre_map.get(g.game_key, []),
            "categories":  category_map.get(g.game_key, []),
            "developers":  developer_map.get(g.game_key, []),
            "publishers":  publisher_map.get(g.game_key, []),
            "languages":   language_map.get(g.game_key, []),
            "price_snapshots":      price_map.get(g.game_key, []),
            "review_summary":       review_summary_map.get(g.game_key),
            "concurrent_players":   ccu_map.get(g.game_key, []),
            "news":                 news_map.get(g.game_key, []),
            "achievements": {
                **ach_summary_map[g.game_key],
                "global_percentages": ach_global_map.get(g.game_key, []),
            } if g.game_key in ach_summary_map else {
                "global_percentages": ach_global_map.get(g.game_key, []),
            },
        }
        batch.append(doc)

        if len(batch) >= BATCH_SIZE:
            try:
                col.insert_many(batch, ordered=False)
            except BulkWriteError:
                pass
            inserted += len(batch)
            batch = []
            logger.info(f"  [{inserted:,}/{total:,}] juegos migrados")

    if batch:
        try:
            col.insert_many(batch, ordered=False)
        except BulkWriteError:
            pass
        inserted += len(batch)

    # Índices útiles para consultas
    col.create_index("game_name")
    col.create_index("genres")
    col.create_index("metacritic_score")
    col.create_index("release_date")
    col.create_index([("achievements.total", ASCENDING)])

    logger.info(f"✅ games: {inserted:,} documentos insertados.")


# ── COLECCIÓN: users ──────────────────────────────────────────────────────────

def migrate_users(db, drop: bool = False) -> None:
    col = db["users"]
    if drop:
        col.drop()
        logger.info("Colección 'users' eliminada.")

    logger.info("Migrando colección 'users'...")

    with get_session() as session:
        users = session.execute(text("""
            SELECT
                u.user_key, u.steamid_hash,
                u.visibility_state, u.profile_state, u.persona_state,
                u.country_key, u.account_created_date,
                u.account_created_year, u.account_age_band,
                u.last_logoff_date, u.last_logoff_time_bucket_key,
                u.valid_from, u.is_current, u.is_active,
                c.iso_code as country_iso,
                c.country_name,
                tb.bucket_name as last_logoff_bucket
            FROM dim_user u
            LEFT JOIN dim_country c ON c.country_key = u.country_key
            LEFT JOIN dim_time_bucket tb ON tb.time_bucket_key = u.last_logoff_time_bucket_key
            WHERE u.is_current = 1 AND u.is_active = 1
        """)).fetchall()

        total = len(users)
        logger.info(f"  {total:,} usuarios a migrar.")

        # library snapshots
        lib_snaps = session.execute(text("""
            SELECT user_key, date_key, game_count
            FROM fact_user_library_snapshot
        """)).fetchall()
        lib_map = {r.user_key: {"date_key": r.date_key, "game_count": r.game_count}
                   for r in lib_snaps}

        # owned games
        owned = session.execute(text("""
            SELECT f.user_key, f.game_key, g.appid, g.game_name,
                   f.playtime_forever_min, f.playtime_windows_forever_min,
                   f.playtime_mac_forever_min, f.playtime_linux_forever_min,
                   f.playtime_deck_forever_min, f.rtime_last_played_date,
                   f.has_visible_stats, f.has_leaderboards,
                   f.has_workshop, f.has_market, f.has_dlc
            FROM fact_user_owned_game f
            JOIN dim_game g ON g.game_key = f.game_key AND g.is_current = 1
            ORDER BY f.user_key, f.playtime_forever_min DESC
        """)).fetchall()
        owned_map = {}
        for r in owned:
            owned_map.setdefault(r.user_key, []).append({
                "appid":                r.appid,
                "game_name":            r.game_name,
                "playtime_forever_min": r.playtime_forever_min,
                "playtime_windows_min": r.playtime_windows_forever_min,
                "playtime_mac_min":     r.playtime_mac_forever_min,
                "playtime_linux_min":   r.playtime_linux_forever_min,
                "playtime_deck_min":    r.playtime_deck_forever_min,
                "last_played":          str(r.rtime_last_played_date) if r.rtime_last_played_date else None,
                "has_achievements":     bool(r.has_visible_stats),
                "has_workshop":         bool(r.has_workshop),
                "has_market":           bool(r.has_market),
                "has_dlc":              bool(r.has_dlc),
            })

        # recent play
        recent = session.execute(text("""
            SELECT f.user_key, g.appid, g.game_name,
                   f.playtime_2weeks_min, f.playtime_forever_min, f.date_key
            FROM fact_user_recent_play f
            JOIN dim_game g ON g.game_key = f.game_key AND g.is_current = 1
            ORDER BY f.user_key, f.playtime_2weeks_min DESC
        """)).fetchall()
        recent_map = {}
        for r in recent:
            recent_map.setdefault(r.user_key, []).append({
                "appid":              r.appid,
                "game_name":          r.game_name,
                "playtime_2weeks_min": r.playtime_2weeks_min,
                "playtime_forever_min": r.playtime_forever_min,
                "date_key":           r.date_key,
            })

    batch    = []
    inserted = 0

    for i, u in enumerate(users, 1):
        doc = {
            "_id":              u.steamid_hash,
            "steamid_hash":     u.steamid_hash,
            "user_key":         u.user_key,
            "country_iso":      u.country_iso,
            "country_name":     u.country_name,
            "visibility_state": u.visibility_state,
            "profile_state":    u.profile_state,
            "persona_state":    u.persona_state,
            "account_created_year": u.account_created_year,
            "account_age_band": u.account_age_band,
            "last_logoff_bucket": u.last_logoff_bucket,
            "valid_from":       u.valid_from.isoformat() if u.valid_from else None,
            "library_snapshot": lib_map.get(u.user_key),
            "owned_games":      owned_map.get(u.user_key, []),
            "recent_play":      recent_map.get(u.user_key, []),
        }
        batch.append(doc)

        if len(batch) >= BATCH_SIZE:
            try:
                col.insert_many(batch, ordered=False)
            except BulkWriteError:
                pass
            inserted += len(batch)
            batch = []
            logger.info(f"  [{inserted:,}/{total:,}] usuarios migrados")

    if batch:
        try:
            col.insert_many(batch, ordered=False)
        except BulkWriteError:
            pass
        inserted += len(batch)

    col.create_index("country_iso")
    col.create_index("account_age_band")
    col.create_index([("library_snapshot.game_count", ASCENDING)])

    logger.info(f"✅ users: {inserted:,} documentos insertados.")


# ── COLECCIÓN: review_details ─────────────────────────────────────────────────

def migrate_reviews(db, drop: bool = False) -> None:
    col = db["review_details"]
    if drop:
        col.drop()
        logger.info("Colección 'review_details' eliminada.")

    logger.info("Migrando colección 'review_details'...")

    with get_session() as session:
        total = session.execute(
            text("SELECT COUNT(*) as n FROM fact_game_review_detail")
        ).fetchone().n
        logger.info(f"  {total:,} reseñas a migrar.")

        offset   = 0
        inserted = 0

        while True:
            rows = session.execute(text("""
                SELECT
                    f.recommendation_id, f.game_key,
                    g.appid, g.game_name,
                    l.steam_api_name as language,
                    f.voted_up, f.votes_up, f.votes_funny,
                    f.weighted_vote_score, f.comment_count,
                    f.steam_purchase, f.received_for_free,
                    f.refunded, f.written_during_early_access,
                    f.primarily_steam_deck,
                    f.created_date_key, f.updated_date_key,
                    f.review_text,
                    f.author_playtime_forever,
                    f.author_playtime_last_two_weeks,
                    f.author_playtime_at_review,
                    f.author_last_played
                FROM fact_game_review_detail f
                JOIN dim_game g ON g.game_key = f.game_key AND g.is_current = 1
                LEFT JOIN dim_language l ON l.language_key = f.language_key
                ORDER BY f.recommendation_id
                OFFSET :offset ROWS FETCH NEXT :batch ROWS ONLY
            """), {"offset": offset, "batch": BATCH_SIZE}).fetchall()

            if not rows:
                break

            docs = []
            for r in rows:
                docs.append({
                    "_id":             r.recommendation_id,
                    "recommendation_id": r.recommendation_id,
                    "appid":           r.appid,
                    "game_name":       r.game_name,
                    "language":        r.language,
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
                        "playtime_forever_min":    r.author_playtime_forever,
                        "playtime_2weeks_min":      r.author_playtime_last_two_weeks,
                        "playtime_at_review_min":  r.author_playtime_at_review,
                        "last_played":             r.author_last_played.isoformat() if r.author_last_played else None,
                    }
                })

            try:
                col.insert_many(docs, ordered=False)
            except BulkWriteError:
                pass

            inserted += len(docs)
            offset   += BATCH_SIZE

            if inserted % 50000 == 0:
                logger.info(f"  [{inserted:,}/{total:,}] reseñas migradas")

    col.create_index("appid")
    col.create_index("language")
    col.create_index("voted_up")
    col.create_index([("weighted_vote_score", ASCENDING)])

    logger.info(f"✅ review_details: {inserted:,} documentos insertados.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")

    parser = argparse.ArgumentParser(description="Migración SQL Server → MongoDB")
    parser.add_argument("--collection", choices=["games", "users", "reviews", "all"],
                        default="all", help="Colección a migrar (default: all)")
    parser.add_argument("--drop", action="store_true",
                        help="Eliminar colección antes de migrar")
    args = parser.parse_args()

    db = get_mongo_db()
    logger.info(f"Conectado a MongoDB: {MONGO_URI} / {MONGO_DB}")

    if args.collection in ("games", "all"):
        migrate_games(db, drop=args.drop)

    if args.collection in ("users", "all"):
        migrate_users(db, drop=args.drop)

    if args.collection in ("reviews", "all"):
        migrate_reviews(db, drop=args.drop)

    logger.info("✅ Migración completa.")


if __name__ == "__main__":
    main()