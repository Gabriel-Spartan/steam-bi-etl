# src/etl/steam_appdetails.py
"""
Módulo compartido para obtener y cachear appdetails de Steam.

Estrategia de selección:
    - Top 100 juegos por jugadores concurrentes (ISteamChartsService)
    - 250 juegos indie desde el catálogo (IStoreService + tag Indie)
    - Total: hasta 500 juegos únicos
    - Caché local en data/cache/appdetails_top500.json
"""
import json
import logging
import random
import time
from pathlib import Path

import requests

from src.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

CHARTS_URL  = "https://api.steampowered.com/ISteamChartsService/GetMostPlayedGames/v1/"
APPLIST_URL = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
DETAILS_URL = "https://store.steampowered.com/api/appdetails"
STORE_SEARCH_URL = "https://store.steampowered.com/api/storesearch/"

CACHE_PATH = Path(__file__).resolve().parents[2] / "data" / "cache" / "appdetails_top500.json"

INDIE_TAG_ID = 492  # Tag oficial de Steam para juegos Indie


# ── Caché ─────────────────────────────────────────────────────────────────────

def save_cache(data: list[dict]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"  Caché guardado en {CACHE_PATH} ({len(data)} juegos)")


def load_cache() -> list[dict] | None:
    if not CACHE_PATH.exists():
        return None

    # Verificar que no esté vacío antes de intentar parsear
    if CACHE_PATH.stat().st_size == 0:
        logger.warning(f"  Caché encontrado pero vacío en {CACHE_PATH}, ignorando.")
        CACHE_PATH.unlink()  # elimina el archivo vacío
        return None

    try:
        logger.info(f"  Caché encontrado en {CACHE_PATH}, cargando...")
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"  {len(data)} juegos cargados desde caché.")
        return data
    except json.JSONDecodeError:
        logger.warning(f"  Caché corrupto en {CACHE_PATH}, eliminando y reconstruyendo.")
        CACHE_PATH.unlink()
        return None


def clear_cache() -> None:
    """Elimina el caché para forzar una nueva llamada a la API."""
    if CACHE_PATH.exists():
        CACHE_PATH.unlink()
        logger.info(f"  Caché eliminado: {CACHE_PATH}")


# ── Fuente 1: Top por jugadores concurrentes ──────────────────────────────────

def get_top_appids(limit: int = 250) -> list[int]:
    """Obtiene los appids de los juegos más jugados en este momento."""
    logger.info(f"[Fuente 1] Obteniendo top {limit} juegos por jugadores concurrentes...")
    response = requests.get(
        CHARTS_URL,
        params={"key": settings.steam_api_key},
        timeout=15,
    )
    response.raise_for_status()

    ranks = response.json().get("response", {}).get("ranks", [])
    appids = [r["appid"] for r in ranks if "appid" in r][:limit]
    logger.info(f"  {len(appids)} appids obtenidos del top.")
    return appids


# ── Fuente 2: Juegos indie desde el catálogo ──────────────────────────────────

def get_indie_appids(limit: int = 250) -> list[int]:
    """
    Obtiene candidatos a juegos indie desde el catálogo general.
    Estrategia: appids recientes (>1,000,000) tienen mayor densidad
    de juegos indie que appids históricos.
    """
    logger.info("[Fuente 2] Obteniendo candidatos indie desde catálogo reciente...")
    all_appids = []
    last_appid = 0

    # Paginamos hasta llegar a appids recientes o tener suficientes
    while len(all_appids) < limit * 6:
        try:
            response = requests.get(
                APPLIST_URL,
                params={
                    "key":           settings.steam_api_key,
                    "include_games": "true",
                    "include_dlc":   "false",
                    "max_results":   50000,
                    "last_appid":    last_appid,
                },
                timeout=30,
            )
            response.raise_for_status()
            body = response.json().get("response", {})
            apps = body.get("apps", [])

            if not apps:
                break

            # Solo appids modernos (>800,000) — mayor densidad indie
            modern = [a["appid"] for a in apps if a.get("appid", 0) > 800_000]
            all_appids.extend(modern)
            last_appid = body.get("last_appid", 0)

            logger.info(
                f"  Lote procesado: {len(apps)} apps, "
                f"{len(modern)} modernos, acumulado: {len(all_appids)}"
            )

            if not body.get("have_more_results", False):
                break

            time.sleep(0.5)

        except requests.RequestException as e:
            logger.warning(f"  Error: {e}")
            break

    logger.info(f"  {len(all_appids)} appids modernos encontrados.")

    # Muestra aleatoria para diversidad
    sample_size = min(limit * 3, len(all_appids))
    sampled = random.sample(all_appids, sample_size) if len(all_appids) > sample_size else all_appids
    logger.info(f"  {len(sampled)} appids seleccionados como candidatos.")
    return sampled

# ── API appdetails ─────────────────────────────────────────────────────────────

def get_appdetails_with_ratelimit(appid: int, max_retries: int = 5) -> dict | None:
    """
    Llama a appdetails con manejo específico de rate limit 429.
    En caso de 429 espera progresivamente más tiempo antes de reintentar.
    """
    wait_times = [10, 30, 60, 120, 180]  # segundos de espera por intento

    for attempt, wait in enumerate(wait_times[:max_retries], 1):
        try:
            response = requests.get(
                DETAILS_URL,
                params={
                    "appids": appid,
                    "cc":     settings.steam_country,
                    "l":      settings.steam_lang,
                },
                timeout=15,
            )

            if response.status_code == 429:
                logger.warning(
                    f"  appid {appid}: 429 rate limit (intento {attempt}/{max_retries}), "
                    f"esperando {wait}s..."
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            data = response.json()

            app_data = data.get(str(appid), {})
            if not app_data.get("success"):
                return None

            return app_data.get("data", {})

        except requests.RequestException as e:
            logger.warning(f"  appid {appid} intento {attempt}: {e}")
            if attempt < max_retries:
                time.sleep(wait_times[attempt - 1])

    logger.error(f"  appid {appid}: falló después de {max_retries} intentos.")
    return None


def is_indie(data: dict) -> bool:
    """Verifica si un juego tiene el género Indie en sus géneros."""
    genres = data.get("genres", [])
    return any(int(g.get("id", 0)) == 25 for g in genres)
    # genre_id 25 = Indie en Steam


def fetch_appdetails_list(
    appids: list[int],
    delay: float = 2.5,
    indie_only: bool = False,
    limit: int | None = None,
) -> list[dict]:
    results = []
    total = len(appids)
    consecutive_errors = 0

    for i, appid in enumerate(appids, 1):
        if limit and len(results) >= limit:
            logger.info(f"  Límite de {limit} alcanzado, deteniendo.")
            break

        logger.info(f"  [{i}/{total}] appid {appid} | válidos: {len(results)}...")

        # Si acumulamos muchos errores seguidos, pausa larga
        if consecutive_errors >= 5:
            wait = 60
            logger.warning(f"  Demasiados errores seguidos, esperando {wait}s...")
            time.sleep(wait)
            consecutive_errors = 0

        data = get_appdetails_with_ratelimit(appid)

        if data is None:
            consecutive_errors += 1
            continue

        consecutive_errors = 0

        if indie_only and not is_indie(data):
            continue

        results.append(data)
        time.sleep(delay)

    logger.info(f"  {len(results)} juegos obtenidos.")
    return results

# ── Punto de entrada principal ─────────────────────────────────────────────────

def get_appdetails_cached(force_refresh: bool = False) -> list[dict]:
    """
    Función principal que usan todos los ETLs.

    Estrategia:
        1. Lee caché si existe y force_refresh=False.
        2. Si no hay caché:
            a. Obtiene top 250 por jugadores concurrentes.
            b. Obtiene muestra del catálogo y filtra hasta 250 indie.
            c. Combina, deduplica y guarda caché.

    Uso en ETLs:
        from src.etl.steam_appdetails import get_appdetails_cached
        all_details = get_appdetails_cached()
    """
    if not force_refresh:
        cached = load_cache()
        if cached is not None:
            return cached

    logger.info("=" * 60)
    logger.info("Construyendo caché desde Steam API...")
    logger.info("Esto puede tardar entre 15 y 20 minutos.")
    logger.info("=" * 60)

    # ── Paso 1: top 250 por CCU ───────────────────────────────────────────────
    top_appids = get_top_appids(250)
    logger.info(f"\nObteniendo appdetails del top {len(top_appids)}...")
    top_details = fetch_appdetails_list(top_appids, delay=1.5)
    top_steam_appids = {d["steam_appid"] for d in top_details}
    logger.info(f"Top CCU: {len(top_details)} juegos obtenidos.")

    # ── Paso 2: 250 indie desde el catálogo ──────────────────────────────────
    logger.info("\nObteniendo muestra de juegos indie...")
    indie_candidates = get_indie_appids(250)

    # Excluir los que ya tenemos del top
    indie_candidates = [a for a in indie_candidates if a not in top_steam_appids]
    logger.info(f"{len(indie_candidates)} candidatos indie (sin duplicados con top CCU).")

    logger.info(f"\nObteniendo appdetails de candidatos indie (hasta 250 válidos)...")
    indie_details = fetch_appdetails_list(
        indie_candidates,
        delay=2.5,
        indie_only=True,
        limit=250,
    )
    logger.info(f"Indie: {len(indie_details)} juegos obtenidos.")

    # ── Paso 3: combinar y guardar ────────────────────────────────────────────
    all_details = top_details + indie_details
    logger.info(f"\nTotal combinado: {len(all_details)} juegos únicos.")
    save_cache(all_details)

    return all_details
