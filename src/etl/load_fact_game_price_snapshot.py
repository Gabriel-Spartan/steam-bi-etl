# src/etl/load_fact_game_price_snapshot.py
"""
Carga fact_game_price_snapshot usando appdetails price_overview.
Solo procesa juegos que están en bibliotecas de usuarios.
Guarda progreso en data/cache/price_progress.json.

Fuente: store.steampowered.com/api/appdetails
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

DETAILS_URL      = "https://store.steampowered.com/api/appdetails"
DELAY_BETWEEN    = 1.5
DELAY_ON_429     = 60
CHECKPOINT_EVERY = 500

PROGRESS_PATH = Path(__file__).resolve().parents[2] / "data" / "cache" / "price_progress.json"


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

SQL_GET_CURRENCY_KEY = text("""
    SELECT currency_key FROM dim_currency
    WHERE currency_code = :currency_code
""")

SQL_GET_COUNTRY_KEY = text("""
    SELECT country_key FROM dim_country
    WHERE iso_code = :iso_code
""")

SQL_INSERT = text("""
    MERGE fact_game_price_snapshot AS target
    USING (VALUES (
        :game_key, :country_key, :currency_key, :date_key,
        :captured_at, :initial_price, :final_price,
        :discount_percent, :etl_run_id
    )) AS source (
        game_key, country_key, currency_key, date_key,
        captured_at, initial_price, final_price,
        discount_percent, etl_run_id
    )
    ON target.game_key    = source.game_key
   AND target.country_key = source.country_key
   AND target.currency_key = source.currency_key
   AND target.captured_at = source.captured_at
    WHEN NOT MATCHED THEN
        INSERT (
            game_key, country_key, currency_key, date_key,
            captured_at, initial_price, final_price,
            discount_percent, etl_run_id
        )
        VALUES (
            source.game_key, source.country_key, source.currency_key,
            source.date_key, source.captured_at, source.initial_price,
            source.final_price, source.discount_percent, source.etl_run_id
        );
""")


# ── API ───────────────────────────────────────────────────────────────────────

def get_price(appid: int, retry: bool = True) -> dict | None:
    """
    Obtiene price_overview de un juego para el país configurado.
    Devuelve None si el juego es F2P, no tiene precio o hay error.
    """
    try:
        r = requests.get(
            DETAILS_URL,
            params={
                "appids": appid,
                "cc":     settings.steam_country,
                "l":      settings.steam_lang,
                "filters": "price_overview",  # solo trae price_overview, más rápido
            },
            timeout=15,
        )

        if r.status_code == 429:
            if retry:
                logger.warning(f"  429 appid {appid}, esperando {DELAY_ON_429}s...")
                time.sleep(DELAY_ON_429)
                return get_price(appid, retry=False)
            return None

        if r.status_code in (400, 403, 404):
            return None

        r.raise_for_status()
        data     = r.json()
        app_data = data.get(str(appid), {})

        if not app_data.get("success"):
            return None

        return app_data.get("data", {}).get("price_overview")

    except requests.exceptions.ConnectionError:
        logger.warning(f"  Error de red appid {appid}, esperando 15s...")
        time.sleep(15)
        if retry:
            return get_price(appid, retry=False)
        return None

    except requests.RequestException as e:
        logger.warning(f"  Error appid {appid}: {e}")
        return None


# ── Load ──────────────────────────────────────────────────────────────────────

def load() -> None:
    run_id   = start_etl_run("load_fact_game_price_snapshot")
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
            game_map = {row.appid: row.game_key for row in game_rows}

            # country_key para el país configurado en .env
            country_row = session.execute(
                SQL_GET_COUNTRY_KEY,
                {"iso_code": settings.steam_country.upper()}
            ).fetchone()
            country_key = country_row.country_key if country_row else None

        if not country_key:
            logger.error(f"  País '{settings.steam_country}' no encontrado en dim_country.")
            finish_etl_run(run_id, status="failed",
                           error_message=f"country_key no encontrado para {settings.steam_country}")
            return

        logger.info(f"  country_key para {settings.steam_country}: {country_key}")

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

        # Paso 4: obtener precios
        captured_at = datetime.now()
        date_key    = int(captured_at.strftime("%Y%m%d"))
        batch       = []

        # Caché de currency_key por código para no hacer query por cada juego
        currency_cache = {}

        for i, appid in enumerate(pending, 1):
            game_key = game_map.get(appid)
            if not game_key:
                skipped += 1
                done_appids.add(appid)
                continue

            price = get_price(appid)

            if not price:
                skipped += 1
                done_appids.add(appid)
                time.sleep(DELAY_BETWEEN)
                continue

            # Obtener currency_key (con caché local)
            currency_code = price.get("currency", "").upper()
            if currency_code not in currency_cache:
                with get_session() as session:
                    cur_row = session.execute(
                        SQL_GET_CURRENCY_KEY,
                        {"currency_code": currency_code}
                    ).fetchone()
                currency_cache[currency_code] = cur_row.currency_key if cur_row else None

            currency_key = currency_cache.get(currency_code)
            if not currency_key:
                logger.warning(f"  Moneda '{currency_code}' no en dim_currency, appid {appid}")
                skipped += 1
                done_appids.add(appid)
                time.sleep(DELAY_BETWEEN)
                continue

            batch.append({
                "game_key":        game_key,
                "country_key":     country_key,
                "currency_key":    currency_key,
                "date_key":        date_key,
                "captured_at":     captured_at,
                "initial_price":   price.get("initial", 0),
                "final_price":     price.get("final", 0),
                "discount_percent": price.get("discount_percent", 0),
                "etl_run_id":      run_id,
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
            f"✅ fact_game_price_snapshot completado:"
            f"\n  Precios insertados: {inserted:,}"
            f"\n  Sin precio (skip):  {skipped:,}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()
