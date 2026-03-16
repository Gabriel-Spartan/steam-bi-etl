# src/etl/load_fact_game_review_summary.py
"""
Carga fact_game_review_summary usando appreviews query_summary.
Solo procesa juegos que están en bibliotecas de usuarios.
Guarda progreso en data/cache/review_summary_progress.json.

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
DELAY_BETWEEN    = 1.0
DELAY_ON_429     = 30
CHECKPOINT_EVERY = 500

PROGRESS_PATH = Path(__file__).resolve().parents[2] / "data" / "cache" / "review_summary_progress.json"


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

SQL_INSERT = text("""
    MERGE fact_game_review_summary AS target
    USING (VALUES (
        :game_key, :date_key, :review_score,
        :review_score_desc, :total_positive,
        :total_negative, :total_reviews, :etl_run_id
    )) AS source (
        game_key, date_key, review_score,
        review_score_desc, total_positive,
        total_negative, total_reviews, etl_run_id
    )
    ON target.game_key = source.game_key
   AND target.date_key = source.date_key
    WHEN MATCHED THEN
        UPDATE SET
            review_score      = source.review_score,
            review_score_desc = source.review_score_desc,
            total_positive    = source.total_positive,
            total_negative    = source.total_negative,
            total_reviews     = source.total_reviews,
            etl_run_id        = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (
            game_key, date_key, review_score,
            review_score_desc, total_positive,
            total_negative, total_reviews, etl_run_id
        )
        VALUES (
            source.game_key, source.date_key, source.review_score,
            source.review_score_desc, source.total_positive,
            source.total_negative, source.total_reviews, source.etl_run_id
        );
""")


# ── API ───────────────────────────────────────────────────────────────────────

def get_review_summary(appid: int, retry: bool = True) -> dict | None:
    """
    Obtiene el resumen agregado de reseñas de un juego.
    Devuelve None si no hay reseñas o hay error.
    """
    try:
        r = requests.get(
            REVIEWS_URL.format(appid=appid),
            params={
                "json":       1,
                "language":   "all",
                "purchase_type": "all",
                "num_per_page": 0,  # solo queremos query_summary, no reseñas
            },
            timeout=15,
        )

        if r.status_code == 429:
            if retry:
                logger.warning(f"  429 appid {appid}, esperando {DELAY_ON_429}s...")
                time.sleep(DELAY_ON_429)
                return get_review_summary(appid, retry=False)
            return None

        if r.status_code in (400, 403, 404):
            return None

        r.raise_for_status()
        data    = r.json()
        summary = data.get("query_summary", {})

        if not summary or summary.get("total_reviews", 0) == 0:
            return None

        return summary

    except requests.exceptions.ConnectionError:
        logger.warning(f"  Error de red appid {appid}, esperando 15s...")
        time.sleep(15)
        if retry:
            return get_review_summary(appid, retry=False)
        return None

    except requests.RequestException as e:
        logger.warning(f"  Error appid {appid}: {e}")
        return None


# ── Load ──────────────────────────────────────────────────────────────────────

def load() -> None:
    run_id   = start_etl_run("load_fact_game_review_summary")
    inserted = 0
    skipped  = 0

    try:
        # Paso 1: appids de bibliotecas
        logger.info("Leyendo appids desde user_libraries.jsonl...")
        appids = get_unique_appids_from_jsonl()
        logger.info(f"  {len(appids):,} appids únicos.")

        # Paso 2: game_map en memoria
        logger.info("Cargando game_map en memoria...")
        with get_session() as session:
            rows = session.execute(
                text("SELECT appid, game_key FROM dim_game WHERE is_current = 1")
            ).fetchall()
        game_map = {row.appid: row.game_key for row in rows}

        # Paso 3: filtrar pendientes
        done_appids = load_progress()
        pending     = [a for a in appids if a not in done_appids]
        logger.info(
            f"  Ya procesados: {len(done_appids):,} | "
            f"Pendientes: {len(pending):,}"
        )

        if not pending:
            logger.info("  Nada pendiente.")
            finish_etl_run(run_id, status="success")
            return

        # Paso 4: obtener resúmenes
        date_key = int(datetime.now().strftime("%Y%m%d"))
        batch    = []

        for i, appid in enumerate(pending, 1):
            game_key = game_map.get(appid)
            if not game_key:
                skipped += 1
                done_appids.add(appid)
                continue

            summary = get_review_summary(appid)

            if not summary:
                skipped += 1
                done_appids.add(appid)
                time.sleep(DELAY_BETWEEN)
                continue

            batch.append({
                "game_key":         game_key,
                "date_key":         date_key,
                "review_score":     summary.get("review_score"),
                "review_score_desc": (summary.get("review_score_desc") or "").strip()[:50],
                "total_positive":   summary.get("total_positive", 0),
                "total_negative":   summary.get("total_negative", 0),
                "total_reviews":    summary.get("total_reviews", 0),
                "etl_run_id":       run_id,
            })

            done_appids.add(appid)

            # Insertar en lotes de 200
            if len(batch) >= 200:
                with get_session() as session:
                    session.execute(SQL_INSERT, batch)
                inserted += len(batch)
                batch = []

            # Checkpoint cada 500
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
            f"✅ fact_game_review_summary completado:"
            f"\n  Resúmenes insertados: {inserted:,}"
            f"\n  Sin reseñas (skip):   {skipped:,}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()