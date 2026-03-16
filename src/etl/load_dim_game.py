# src/etl/load_dim_game.py
"""
Carga dim_game desde SteamSpy /all (primera pasada masiva).
Campos que SteamSpy no provee quedan NULL para enriquecer
después con appdetails.

Fuente: https://steamspy.com/api.php?request=all&page=N
Total estimado: ~86,543 juegos en ~87 páginas
"""
import logging
import time
from pathlib import Path
import json

import requests
from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run

logger = logging.getLogger(__name__)

STEAMSPY_URL   = "https://steamspy.com/api.php"
CACHE_PATH     = Path(__file__).resolve().parents[2] / "data" / "cache" / "steamspy_all.json"
CHECKPOINT_EVERY = 5000
DELAY_PAGES    = 1.0  # segundos entre páginas


# ── Caché ─────────────────────────────────────────────────────────────────────

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


def load_steamspy_cache() -> list[dict] | None:
    if not CACHE_PATH.exists() or CACHE_PATH.stat().st_size == 0:
        return None
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"  Caché SteamSpy encontrado: {len(data)} juegos.")
        return data
    except json.JSONDecodeError:
        logger.warning("  Caché SteamSpy corrupto, descargando de nuevo.")
        CACHE_PATH.unlink()
        return None


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_all_steamspy() -> list[dict]:
    """
    Descarga todas las páginas de SteamSpy /all y devuelve
    lista plana de dicts con los datos de cada juego.
    """
    cached = load_steamspy_cache()
    if cached is not None:
        return cached

    logger.info("Descargando catálogo completo de SteamSpy...")
    all_games = []
    page = 0

    while True:
        try:
            r = requests.get(
                STEAMSPY_URL,
                params={"request": "all", "page": page},
                timeout=30,
            )
            r.raise_for_status()
            data = r.json()

            if not data:
                logger.info(f"  Página {page} vacía, fin del catálogo.")
                break

            games = list(data.values())
            all_games.extend(games)
            logger.info(f"  Página {page}: {len(games)} juegos | acumulado: {len(all_games)}")

            if len(games) < 1000:
                logger.info(f"  Última página en {page}.")
                break

            page += 1
            time.sleep(DELAY_PAGES)

        except requests.RequestException as e:
            logger.warning(f"  Error en página {page}: {e}, reintentando en 30s...")
            time.sleep(30)
            continue

    logger.info(f"  Descarga completa: {len(all_games)} juegos.")
    _atomic_save(CACHE_PATH, all_games)
    return all_games


# ── Transform ─────────────────────────────────────────────────────────────────

def parse_owners_low(owners_str: str) -> int | None:
    """
    Parsea el rango de propietarios de SteamSpy.
    '100,000,000 .. 200,000,000' → 100000000
    """
    if not owners_str:
        return None
    try:
        low = owners_str.split("..")[0].strip().replace(",", "")
        return int(low)
    except (ValueError, IndexError):
        return None


def extract_game_row(game: dict, run_id: int) -> dict:
    """
    Extrae los campos de dim_game desde un registro de SteamSpy.
    Campos no disponibles en SteamSpy quedan NULL.
    """
    return {
        "appid":                  game.get("appid"),
        "game_name":              (game.get("name") or "").strip()[:255] or "Unknown",
        # Campos que SteamSpy no provee → NULL para enriquecer con appdetails
        "game_type":              None,
        "required_age":           None,
        "is_free":                1 if str(game.get("price", "1")) == "0" else None,
        "controller_support":     None,
        "website":                None,
        "release_date":           None,
        "coming_soon":            None,
        "recommendations_total":  None,
        "achievements_total":     None,
        "metacritic_score":       None,
        "platform_windows":       None,
        "platform_mac":           None,
        "platform_linux":         None,
        "last_modified_ts":       None,
        "price_change_number":    None,
        "etl_run_id":             run_id,
    }


# ── SQL ───────────────────────────────────────────────────────────────────────

SQL_EXISTS = text("""
    SELECT game_key FROM dim_game
    WHERE appid = :appid AND is_current = 1
""")

SQL_INSERT = text("""
    INSERT INTO dim_game (
        appid, game_name, game_type, required_age, is_free,
        controller_support, website, release_date, coming_soon,
        recommendations_total, achievements_total, metacritic_score,
        platform_windows, platform_mac, platform_linux,
        last_modified_ts, price_change_number,
        etl_run_id, valid_from, valid_to, is_current, is_active
    )
    VALUES (
        :appid, :game_name, :game_type, :required_age, :is_free,
        :controller_support, :website, :release_date, :coming_soon,
        :recommendations_total, :achievements_total, :metacritic_score,
        :platform_windows, :platform_mac, :platform_linux,
        :last_modified_ts, :price_change_number,
        :etl_run_id, GETDATE(), NULL, 1, 1
    )
""")


# ── Load ──────────────────────────────────────────────────────────────────────

def load() -> None:
    run_id   = start_etl_run("load_dim_game_steamspy")
    inserted = 0
    skipped  = 0

    try:
        games = fetch_all_steamspy()
        total = len(games)
        logger.info(f"Cargando {total} juegos en dim_game...")

        with get_session() as session:
            for i, game in enumerate(games, 1):
                appid = game.get("appid")
                if not appid:
                    skipped += 1
                    continue

                # Skip si ya existe versión actual
                existing = session.execute(
                    SQL_EXISTS, {"appid": appid}
                ).fetchone()

                if existing:
                    skipped += 1
                    continue

                row = extract_game_row(game, run_id)
                session.execute(SQL_INSERT, row)
                inserted += 1

                if i % CHECKPOINT_EVERY == 0:
                    session.flush()
                    logger.info(
                        f"  [{i}/{total}] "
                        f"insertados={inserted} skip={skipped}"
                    )

        finish_etl_run(
            run_id, status="success",
            rows_inserted=inserted,
            rows_skipped=skipped,
        )
        logger.info(
            f"dim_game: {inserted} insertados, {skipped} ya existían."
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()