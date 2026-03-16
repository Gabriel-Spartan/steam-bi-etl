# src/etl/collect_user_libraries.py
"""
Recolecta las bibliotecas de todos los usuarios usando GetOwnedGames.
Usa dos API keys en round-robin para duplicar el throughput.
Guarda resultado en data/cache/user_libraries.json.

Sirve para:
  - enrich_dim_game.py            → appids únicos para enriquecer dim_game
  - load_fact_user_owned_game.py  → datos de biblioteca por usuario
"""
import hashlib
import json
import logging
import time
from pathlib import Path

import requests

from src.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

OWNED_GAMES_URL      = "https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/"
LIBRARIES_CACHE = Path(__file__).resolve().parents[2] / "data" / "cache" / "user_libraries.jsonl"
LIBRARIES_PROGRESS   = Path(__file__).resolve().parents[2] / "data" / "cache" / "libraries_progress.json"
FAILED_STEAMIDS_PATH = Path(__file__).resolve().parents[2] / "data" / "cache" / "libraries_failed.json"
USERS_PROGRESS       = Path(__file__).resolve().parents[2] / "data" / "cache" / "users_progress.json"

CHECKPOINT_EVERY   = 100
DELAY_BETWEEN_REQS = 0.5
DELAY_ON_RATELIMIT = 30

# Construir lista de keys disponibles
_API_KEYS = [
    k for k in [
        settings.steam_api_key,
        settings.steam_api_key_2,
        settings.steam_api_key_3,
        settings.steam_api_key_4,
    ]
    if k
]
logger.debug(f"API keys disponibles: {len(_API_KEYS)}")


def _get_key(request_number: int) -> str:
    """Rota entre las keys disponibles en round-robin."""
    return _API_KEYS[request_number % len(_API_KEYS)]


# ── Escritura atómica ─────────────────────────────────────────────────────────

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

def save_library_entry(steam_hash: str, entry: dict) -> None:
    """Agrega una entrada al archivo JSONL sin cargar todo en memoria."""
    LIBRARIES_CACHE.parent.mkdir(parents=True, exist_ok=True)
    with open(LIBRARIES_CACHE, "a", encoding="utf-8") as f:
        f.write(json.dumps({steam_hash: entry}, ensure_ascii=False) + "\n")

# ── Caché y progreso ──────────────────────────────────────────────────────────

def load_libraries_cache() -> dict:
    """Carga el caché JSONL línea por línea para no saturar memoria."""
    if not LIBRARIES_CACHE.exists() or LIBRARIES_CACHE.stat().st_size == 0:
        return {}
    libraries = {}
    with open(LIBRARIES_CACHE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    libraries.update(json.loads(line))
                except json.JSONDecodeError:
                    continue
    logger.info(f"  {len(libraries)} usuarios cargados desde caché JSONL.")
    return libraries

def get_unique_appids_from_jsonl() -> list[int]:
    """
    Extrae appids únicos leyendo el JSONL línea por línea.
    Nunca carga todo en memoria.
    """
    if not LIBRARIES_CACHE.exists():
        return []
    appids = set()
    with open(LIBRARIES_CACHE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                for user_data in entry.values():
                    for game in user_data.get("games", []):
                        if game.get("appid"):
                            appids.add(game["appid"])
            except json.JSONDecodeError:
                continue
    logger.info(f"  {len(appids)} appids únicos en todas las bibliotecas.")
    return list(appids)

def load_progress() -> dict:
    if not LIBRARIES_PROGRESS.exists() or LIBRARIES_PROGRESS.stat().st_size == 0:
        return {"processed_steamids": []}
    try:
        with open(LIBRARIES_PROGRESS, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {"processed_steamids": []}


def load_failed() -> list[str]:
    if not FAILED_STEAMIDS_PATH.exists() or FAILED_STEAMIDS_PATH.stat().st_size == 0:
        return []
    try:
        with open(FAILED_STEAMIDS_PATH, "r", encoding="utf-8") as f:
            return json.load(f).get("failed", [])
    except json.JSONDecodeError:
        return []


# ── API ───────────────────────────────────────────────────────────────────────

def get_owned_games(steamid: str, request_number: int = 0, retry: bool = True) -> tuple[list[dict] | None, str]:
    api_key = _get_key(request_number)

    try:
        r = requests.get(
            OWNED_GAMES_URL,
            params={
                "key":                       api_key,
                "steamid":                   steamid,
                "include_appinfo":           1,
                "include_played_free_games": 1,
                "include_extended_appinfo":  1,
            },
            timeout=15,
        )

        if r.status_code in (401, 403):
            return None, "private"

        if r.status_code in (420, 429):
            if retry:
                logger.warning(
                    f"  {r.status_code} steamid {steamid} "
                    f"(key ...{api_key[-6:]}), esperando {DELAY_ON_RATELIMIT}s..."
                )
                time.sleep(DELAY_ON_RATELIMIT)
                return get_owned_games(steamid, request_number + 1, retry=False)
            else:
                logger.error(f"  Rate limit persistente en {steamid}, guardando para retry.")
                return None, "failed"

        r.raise_for_status()
        games = r.json().get("response", {}).get("games", [])
        return (games, "ok") if games else (None, "private")

    except requests.exceptions.ConnectionError as e:
        # Error de red/DNS → esperar y reintentar, NO marcar como fallido
        logger.warning(f"  Error de red steamid {steamid}, esperando 30s...")
        time.sleep(30)
        if retry:
            return get_owned_games(steamid, request_number, retry=False)
        return None, "failed"

    except requests.RequestException as e:
        logger.warning(f"  Error steamid {steamid}: {e}")
        return None, "failed"


def normalize_game(game: dict) -> dict:
    return {
        "appid":                    game.get("appid"),
        "name":                     game.get("name", ""),
        "playtime_forever":         game.get("playtime_forever", 0),
        "playtime_windows_forever": game.get("playtime_windows_forever", 0),
        "playtime_mac_forever":     game.get("playtime_mac_forever", 0),
        "playtime_linux_forever":   game.get("playtime_linux_forever", 0),
        "playtime_deck_forever":    game.get("playtime_deck_forever", 0),
        "rtime_last_played":        game.get("rtime_last_played", 0),
        "has_visible_stats":        game.get("has_community_visible_stats", False),
        "has_leaderboards":         game.get("has_leaderboard", False),
        "has_workshop":             game.get("has_workshop", False),
        "has_market":               game.get("has_market", False),
        "has_dlc":                  game.get("has_dlc_available", False),
        "playtime_disconnected":    game.get("playtime_disconnected", 0),
    }


# ── Recolector ────────────────────────────────────────────────────────────────

def collect(steamids: list[str] | None = None) -> dict:
    # Ya NO cargamos todo el caché en memoria al inicio
    # Solo cargamos los steam_hashes ya procesados desde el progress
    progress          = load_progress()
    already_processed = set(progress["processed_steamids"])
    failed_previous   = load_failed()

    # Obtener hashes ya en el JSONL para evitar duplicados
    existing_hashes = set()
    if LIBRARIES_CACHE.exists():
        with open(LIBRARIES_CACHE, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        existing_hashes.update(json.loads(line).keys())
                    except json.JSONDecodeError:
                        continue
        logger.info(f"  {len(existing_hashes)} usuarios ya en JSONL.")

    if steamids is None:
        if not USERS_PROGRESS.exists():
            logger.error("No se encontró users_progress.json.")
            return {}
        with open(USERS_PROGRESS, "r", encoding="utf-8") as f:
            steamids = json.load(f).get("collected", [])
        logger.info(f"  {len(steamids)} steamids cargados.")

    failed_pending = [s for s in failed_previous if s not in already_processed]
    pending_normal = [s for s in steamids if s not in already_processed and s not in failed_pending]
    pending        = failed_pending + pending_normal

    logger.info(
        f"Total: {len(steamids)} | "
        f"Ya procesados: {len(already_processed)} | "
        f"Pendientes: {len(pending)} "
        f"({len(failed_pending)} reintentos + {len(pending_normal)} normales)"
    )
    logger.info(f"Keys activas: {len(_API_KEYS)} → throughput x{len(_API_KEYS)}")

    if not pending:
        logger.info("  Nada pendiente.")
        return {}

    accessible      = 0
    private         = 0
    failed_now      = []
    request_counter = 0

    for i, steamid in enumerate(pending, 1):
        games_raw, status = get_owned_games(steamid, request_counter)
        request_counter += 1

        if status == "ok":
            steam_hash = hashlib.sha256(steamid.encode()).hexdigest()
            if steam_hash not in existing_hashes:
                entry = {
                    "steamid": steamid,
                    "games":   [normalize_game(g) for g in games_raw],
                }
                save_library_entry(steam_hash, entry)  # append directo, sin cargar todo
                existing_hashes.add(steam_hash)
            accessible += 1
            already_processed.add(steamid)

        elif status == "failed":
            failed_now.append(steamid)

        else:
            private += 1
            already_processed.add(steamid)

        if i % CHECKPOINT_EVERY == 0:
            _atomic_save(LIBRARIES_PROGRESS, {"processed_steamids": list(already_processed)})
            _atomic_save(FAILED_STEAMIDS_PATH, {"failed": failed_now})
            logger.info(
                f"  [{i}/{len(pending)}] "
                f"accesibles={accessible} privadas={private} "
                f"fallidos={len(failed_now)} en_jsonl={len(existing_hashes)}"
            )

        time.sleep(DELAY_BETWEEN_REQS)

    # Checkpoint final
    _atomic_save(LIBRARIES_PROGRESS, {"processed_steamids": list(already_processed)})
    _atomic_save(FAILED_STEAMIDS_PATH, {"failed": failed_now})

    logger.info(
        f"\n✅ Completado:"
        f"\n  Accesibles:  {accessible}"
        f"\n  Privadas:    {private}"
        f"\n  Fallidos:    {len(failed_now)}"
        f"\n  Total JSONL: {len(existing_hashes)} usuarios"
    )
    return {}

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    collect()

    # Estadísticas leyendo el JSONL línea por línea (sin cargar todo en memoria)
    appids = get_unique_appids_from_jsonl()
    print(f"\nAppids únicos: {len(appids)}")

    from collections import Counter
    counter = Counter()
    if LIBRARIES_CACHE.exists():
        with open(LIBRARIES_CACHE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    for user_data in entry.values():
                        for game in user_data.get("games", []):
                            if game.get("appid"):
                                counter[game["appid"]] += 1
                except json.JSONDecodeError:
                    continue

    print(f"\nTop 10 juegos más comunes:")
    for appid, count in counter.most_common(10):
        print(f"  appid={appid}: {count} usuarios")