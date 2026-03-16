# src/etl/load_fact_news_events.py
"""
Carga fact_news_events usando GetNewsForApp.
Solo procesa juegos que están en bibliotecas de usuarios.
Guarda progreso en data/cache/news_progress.json.

Fuente: ISteamNews/GetNewsForApp/v2
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

NEWS_URL         = "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"
DELAY_BETWEEN    = 1.0
DELAY_ON_429     = 30
CHECKPOINT_EVERY = 500
NEWS_PER_APP     = 10    # últimas 10 noticias por juego
MAX_LENGTH       = 500   # caracteres del contenido

PROGRESS_PATH = Path(__file__).resolve().parents[2] / "data" / "cache" / "news_progress.json"


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
    MERGE fact_news_events AS target
    USING (VALUES (
        :game_key, :news_gid, :date_published,
        :date_key, :title, :feed_label, :url,
        :author, :contents_short, :etl_run_id
    )) AS source (
        game_key, news_gid, date_published,
        date_key, title, feed_label, url,
        author, contents_short, etl_run_id
    )
    ON target.news_gid = source.news_gid
    WHEN NOT MATCHED THEN
        INSERT (
            game_key, news_gid, date_published,
            date_key, title, feed_label, url,
            author, contents_short, etl_run_id
        )
        VALUES (
            source.game_key, source.news_gid,
            source.date_published, source.date_key, source.title,
            source.feed_label, source.url, source.author,
            source.contents_short, source.etl_run_id
        );
""")


# ── API ───────────────────────────────────────────────────────────────────────

def get_news(appid: int, retry: bool = True) -> list[dict]:
    """Obtiene las últimas noticias de un juego."""
    try:
        r = requests.get(
            NEWS_URL,
            params={
                "appid":     appid,
                "count":     NEWS_PER_APP,
                "maxlength": MAX_LENGTH,
                "format":    "json",
            },
            timeout=15,
        )

        if r.status_code == 429:
            if retry:
                logger.warning(f"  429 appid {appid}, esperando {DELAY_ON_429}s...")
                time.sleep(DELAY_ON_429)
                return get_news(appid, retry=False)
            return []

        if r.status_code in (400, 403, 404):
            return []

        r.raise_for_status()
        return r.json().get("appnews", {}).get("newsitems", [])

    except requests.exceptions.ConnectionError:
        logger.warning(f"  Error de red appid {appid}, esperando 15s...")
        time.sleep(15)
        if retry:
            return get_news(appid, retry=False)
        return []

    except requests.RequestException as e:
        logger.warning(f"  Error appid {appid}: {e}")
        return []


def parse_news_row(item, game_key, appid, run_id):
    gid = str(item.get("gid", "")).strip()
    if not gid:
        return None

    ts = item.get("date", 0)
    if ts:
        published_dt = datetime.fromtimestamp(ts)
        date_key     = int(published_dt.strftime("%Y%m%d"))
    else:
        published_dt = None
        date_key     = None

    return {
        "game_key":       game_key,
        # "appid":        appid,  ← eliminar esta línea
        "news_gid":       gid[:100],
        "date_published": published_dt,
        "date_key":       date_key,
        "title":          (item.get("title") or "").strip()[:500],
        "feed_label":     (item.get("feedlabel") or "").strip()[:100],
        "url":            (item.get("url") or "").strip()[:1000],
        "author":         (item.get("author") or "").strip()[:255],
        "contents_short": (item.get("contents") or "").strip()[:2000],
        "etl_run_id":     run_id,
    }


# ── Load ──────────────────────────────────────────────────────────────────────

def load() -> None:
    run_id   = start_etl_run("load_fact_news_events")
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

        # Paso 4: obtener noticias
        batch = []

        for i, appid in enumerate(pending, 1):
            game_key = game_map.get(appid)
            if not game_key:
                skipped += 1
                done_appids.add(appid)
                continue

            news_items = get_news(appid)

            for item in news_items:
                row = parse_news_row(item, game_key, appid, run_id)
                if row:
                    batch.append(row)

            done_appids.add(appid)

            if not news_items:
                skipped += 1

            # Insertar en lotes de 200
            if len(batch) >= 200:
                with get_session() as session:
                    session.execute(SQL_INSERT, batch)
                inserted += len(batch)
                batch = []

            # Checkpoint cada 500 juegos
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
            f"✅ fact_news_events completado:"
            f"\n  Noticias insertadas: {inserted:,}"
            f"\n  Juegos sin noticias: {skipped:,}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()
