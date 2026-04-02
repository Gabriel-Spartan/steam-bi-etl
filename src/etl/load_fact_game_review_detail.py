# src/etl/load_fact_game_review_detail.py
"""
Carga fact_game_review_detail usando appreviews reviews[].
Solo procesa juegos que están en bibliotecas de usuarios.
Guarda progreso en data/cache/review_detail_progress.json.

Fuente: store.steampowered.com/appreviews/{appid}
"""
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import requests
from sqlalchemy import text

from src.config import get_settings
from src.db import get_session, start_etl_run, finish_etl_run
from src.etl.collect_user_libraries import get_unique_appids_from_jsonl

logger = logging.getLogger(__name__)
settings = get_settings()

REVIEWS_URL      = "https://store.steampowered.com/appreviews/{appid}"
DELAY_BETWEEN    = 1.5
DELAY_ON_429     = 60
CHECKPOINT_EVERY = 200
REVIEWS_PER_APP  = 100   # máximo por llamada
MAX_INT = 2_147_483_647

PROGRESS_PATH = Path(__file__).resolve().parents[2] / "data" / "cache" / "review_detail_progress.json"


# ── Progreso ──────────────────────────────────────────────────────────────────

def _atomic_save(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        tmp.replace(path)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def load_progress() -> set:
    if not PROGRESS_PATH.exists() or PROGRESS_PATH.stat().st_size == 0:
        return set()
    try:
        with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
            return set(json.load(f).get("done_appids", []))
    except json.JSONDecodeError:
        return set()


def save_progress(done: set) -> None:
    _atomic_save(PROGRESS_PATH, {"done_appids": list(done)})


# ── SQL ───────────────────────────────────────────────────────────────────────

SQL_GET_LANGUAGE_MAP = text("""
    SELECT language_key, steam_api_name FROM dim_language
    WHERE steam_api_name IS NOT NULL
""")

SQL_INSERT = text("""
    MERGE fact_game_review_detail AS target
    USING (VALUES (
        :recommendation_id, :game_key, :created_date_key,
        :updated_date_key, :language_key, :review_text,
        :voted_up, :votes_up, :votes_funny,
        :weighted_vote_score, :comment_count,
        :steam_purchase, :received_for_free, :refunded,
        :written_during_early_access, :primarily_steam_deck,
        :author_playtime_forever, :author_playtime_last_two_weeks,
        :author_playtime_at_review, :author_last_played,
        :etl_run_id
    )) AS source (
        recommendation_id, game_key, created_date_key,
        updated_date_key, language_key, review_text,
        voted_up, votes_up, votes_funny,
        weighted_vote_score, comment_count,
        steam_purchase, received_for_free, refunded,
        written_during_early_access, primarily_steam_deck,
        author_playtime_forever, author_playtime_last_two_weeks,
        author_playtime_at_review, author_last_played,
        etl_run_id
    )
    ON target.recommendation_id = source.recommendation_id
    WHEN MATCHED THEN
        UPDATE SET
            updated_date_key             = source.updated_date_key,
            votes_up                     = source.votes_up,
            votes_funny                  = source.votes_funny,
            weighted_vote_score          = source.weighted_vote_score,
            comment_count                = source.comment_count,
            etl_run_id                   = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (
            recommendation_id, game_key, created_date_key,
            updated_date_key, language_key, review_text,
            voted_up, votes_up, votes_funny,
            weighted_vote_score, comment_count,
            steam_purchase, received_for_free, refunded,
            written_during_early_access, primarily_steam_deck,
            author_playtime_forever, author_playtime_last_two_weeks,
            author_playtime_at_review, author_last_played,
            etl_run_id
        )
        VALUES (
            source.recommendation_id, source.game_key, source.created_date_key,
            source.updated_date_key, source.language_key, source.review_text,
            source.voted_up, source.votes_up, source.votes_funny,
            source.weighted_vote_score, source.comment_count,
            source.steam_purchase, source.received_for_free, source.refunded,
            source.written_during_early_access, source.primarily_steam_deck,
            source.author_playtime_forever, source.author_playtime_last_two_weeks,
            source.author_playtime_at_review, source.author_last_played,
            source.etl_run_id
        );
""")


# ── API ───────────────────────────────────────────────────────────────────────

def get_reviews(appid: int, cursor: str = "*", retry: bool = True) -> dict | None:
    """
    Obtiene una página de reseñas individuales.
    Devuelve dict con {reviews, cursor} o None si hay error.
    """
    try:
        r = requests.get(
            REVIEWS_URL.format(appid=appid),
            params={
                "json":                   1,
                "language":               "all",
                "purchase_type":          "all",
                "filter":                 "recent",
                "num_per_page":           REVIEWS_PER_APP,
                "cursor":                 cursor,
                "filter_offtopic_activity": 0,
            },
            timeout=15,
        )

        if r.status_code == 429:
            if retry:
                logger.warning(f"  429 appid {appid}, esperando {DELAY_ON_429}s...")
                time.sleep(DELAY_ON_429)
                return get_reviews(appid, cursor, retry=False)
            return None

        if r.status_code in (400, 403, 404):
            return None

        r.raise_for_status()
        data = r.json()

        return {
            "reviews": data.get("reviews", []),
            "cursor":  data.get("cursor", ""),
        }

    except requests.exceptions.ConnectionError:
        logger.warning(f"  Error de red appid {appid}, esperando 15s...")
        time.sleep(15)
        if retry:
            return get_reviews(appid, cursor, retry=False)
        return None

    except requests.RequestException as e:
        logger.warning(f"  Error appid {appid}: {e}")
        return None


# ── Transform ─────────────────────────────────────────────────────────────────

def ts_to_date_key(ts: int) -> int | None:
    if not ts:
        return None
    return int(datetime.fromtimestamp(ts).strftime("%Y%m%d"))


def ts_to_datetime(ts: int):
    if not ts:
        return None
    return datetime.fromtimestamp(ts)


def parse_review(review, game_key, language_map, run_id):
    rec_id = str(review.get("recommendationid", "")).strip()
    if not rec_id:
        return None

    author       = review.get("author", {})
    language_str = review.get("language", "").strip().lower()
    language_key = language_map.get(language_str)

    def safe_int(val, max_val=MAX_INT) -> int:
        """Convierte a int limitando al máximo de INT de SQL Server."""
        try:
            return min(int(val or 0), max_val)
        except (ValueError, TypeError):
            return 0

    return {
        "recommendation_id":           rec_id[:50],
        "game_key":                    game_key,
        "created_date_key":            ts_to_date_key(review.get("timestamp_created")),
        "updated_date_key":            ts_to_date_key(review.get("timestamp_updated")),
        "language_key":                language_key,
        "review_text":                 (review.get("review") or "").strip(),
        "voted_up":                    1 if review.get("voted_up") else 0,
        "votes_up":                    safe_int(review.get("votes_up")),
        "votes_funny":                 safe_int(review.get("votes_funny")),
        "weighted_vote_score":         float(review.get("weighted_vote_score", 0)),
        "comment_count":               safe_int(review.get("comment_count")),
        "steam_purchase":              1 if review.get("steam_purchase") else 0,
        "received_for_free":           1 if review.get("received_for_free") else 0,
        "refunded":                    1 if review.get("refunded") else 0,
        "written_during_early_access": 1 if review.get("written_during_early_access") else 0,
        "primarily_steam_deck":        1 if review.get("primarily_steam_deck") else 0,
        "author_playtime_forever":          safe_int(author.get("playtime_forever")),
        "author_playtime_last_two_weeks":   safe_int(author.get("playtime_last_two_weeks")),
        "author_playtime_at_review":        safe_int(author.get("playtime_at_review")),
        "author_last_played":               ts_to_datetime(author.get("last_played")),
        "etl_run_id":                  run_id,
    }


# ── Load ──────────────────────────────────────────────────────────────────────

def load(max_reviews_per_game: int = 100) -> None:
    """
    max_reviews_per_game: cuántas reseñas cargar por juego.
    100 = solo la primera página (más rápido).
    0   = todas las páginas disponibles (puede ser miles por juego).
    """
    run_id   = start_etl_run("load_fact_game_review_detail")
    inserted = 0
    skipped  = 0

    try:
        # Paso 1: appids de bibliotecas
        logger.info("Leyendo appids desde user_libraries.jsonl...")
        appids = get_unique_appids_from_jsonl()
        logger.info(f"  {len(appids):,} appids únicos.")

        # Paso 2: mapas en memoria
        logger.info("Cargando mapas en memoria...")
        with get_session() as session:
            game_rows = session.execute(
                text("SELECT appid, game_key FROM dim_game WHERE is_current = 1")
            ).fetchall()
            lang_rows = session.execute(SQL_GET_LANGUAGE_MAP).fetchall()

        game_map     = {row.appid: row.game_key for row in game_rows}
        language_map = {row.steam_api_name: row.language_key for row in lang_rows}

        # Paso 3: filtrar pendientes
        done_appids = load_progress()
        pending     = [a for a in appids if a not in done_appids]
        logger.info(
            f"  Ya procesados: {len(done_appids):,} | "
            f"Pendientes: {len(pending):,}"
        )
        logger.info(f"  Max reseñas por juego: {max_reviews_per_game}")

        if not pending:
            logger.info("  Nada pendiente.")
            finish_etl_run(run_id, status="success")
            return

        # Paso 4: obtener reseñas
        batch = []

        for i, appid in enumerate(pending, 1):
            game_key = game_map.get(appid)
            if not game_key:
                skipped += 1
                done_appids.add(appid)
                continue

            # Paginar reseñas
            cursor          = "*"
            reviews_fetched = 0
            has_more        = True

            while has_more:
                result = get_reviews(appid, cursor)

                if not result or not result["reviews"]:
                    break

                for review in result["reviews"]:
                    row = parse_review(review, game_key, language_map, run_id)
                    if row:
                        batch.append(row)
                        reviews_fetched += 1

                # Verificar si hay más páginas
                new_cursor = result.get("cursor", "")
                if (
                    not new_cursor
                    or new_cursor == cursor
                    or len(result["reviews"]) < REVIEWS_PER_APP
                    or (max_reviews_per_game > 0 and reviews_fetched >= max_reviews_per_game)
                ):
                    has_more = False
                else:
                    cursor = new_cursor
                    time.sleep(DELAY_BETWEEN)

            done_appids.add(appid)

            if reviews_fetched == 0:
                skipped += 1

            # Insertar en lotes de 100
            if len(batch) >= 100:
                with get_session() as session:
                    session.execute(SQL_INSERT, batch)
                inserted += len(batch)
                batch = []

            # Checkpoint cada 200 juegos
            if i % CHECKPOINT_EVERY == 0:
                save_progress(done_appids)
                logger.info(
                    f"  [{i:,}/{len(pending):,}] "
                    f"insertados={inserted:,} skip={skipped:,}"
                )

            time.sleep(DELAY_BETWEEN)

        # Insertar restantes
        if batch:
            with get_session() as session:
                session.execute(SQL_INSERT, batch)
            inserted += len(batch)

        # Checkpoint final
        save_progress(done_appids)

        finish_etl_run(
            run_id, status="success",
            rows_inserted=inserted,
            rows_skipped=skipped,
        )
        logger.info(
            f"✅ fact_game_review_detail completado:"
            f"\n  Reseñas insertadas:    {inserted:,}"
            f"\n  Juegos sin reseñas:    {skipped:,}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()