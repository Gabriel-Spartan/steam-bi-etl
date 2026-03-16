# src/etl/steam_users.py
"""
Módulo compartido para obtener steamids desde múltiples fuentes:
  - BFS de amigos partiendo del usuario raíz
  - Miembros de grupos de Steam
"""
import json
import logging
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

from src.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

FRIENDS_URL   = "https://api.steampowered.com/ISteamUser/GetFriendList/v1/"
SUMMARIES_URL = "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/"
GROUP_XML_URL = "https://steamcommunity.com/groups/{group}/memberslistxml/?xml=1&p={page}"

PROGRESS_PATH = Path(__file__).resolve().parents[2] / "data" / "cache" / "users_progress.json"


# ── Progreso ──────────────────────────────────────────────────────────────────

def load_progress() -> dict:
    """Carga el progreso guardado o devuelve estructura vacía."""
    if not PROGRESS_PATH.exists():
        return {"collected": [], "sources_done": []}
    if PROGRESS_PATH.stat().st_size == 0:
        return {"collected": [], "sources_done": []}
    try:
        with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"  Progreso cargado: {len(data['collected'])} steamids ya recolectados.")
        return data
    except json.JSONDecodeError:
        logger.warning("  Progreso corrupto, reiniciando.")
        return {"collected": [], "sources_done": []}


def save_progress(progress: dict) -> None:
    """Guarda el progreso actual en disco."""
    PROGRESS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def clear_progress() -> None:
    """Elimina el progreso para empezar desde cero."""
    if PROGRESS_PATH.exists():
        PROGRESS_PATH.unlink()
        logger.info("  Progreso eliminado.")


# ── API helpers ───────────────────────────────────────────────────────────────

def get_friends(steamid: str) -> list[str]:
    """Obtiene la lista de amigos de un steamid. Devuelve [] si es privado."""
    try:
        r = requests.get(
            FRIENDS_URL,
            params={
                "key":          settings.steam_api_key,
                "steamid":      steamid,
                "relationship": "friend",
            },
            timeout=15,
        )
        if r.status_code == 401:
            return []  # lista de amigos privada
        r.raise_for_status()
        return [f["steamid"] for f in r.json().get("friendslist", {}).get("friends", [])]
    except requests.RequestException:
        return []


def get_player_summaries(steamids: list[str]) -> list[dict]:
    """
    Obtiene perfiles de hasta 100 steamids por llamada.
    Filtra automáticamente solo perfiles públicos.
    """
    if not steamids:
        return []
    public_players = []

    # Steam acepta hasta 100 steamids por llamada
    for i in range(0, len(steamids), 100):
        batch = steamids[i:i + 100]
        try:
            r = requests.get(
                SUMMARIES_URL,
                params={
                    "key":      settings.steam_api_key,
                    "steamids": ",".join(batch),
                },
                timeout=15,
            )
            r.raise_for_status()
            players = r.json().get("response", {}).get("players", [])
            public = [p for p in players if p.get("communityvisibilitystate") == 3]
            public_players.extend(public)
            time.sleep(0.3)
        except requests.RequestException as e:
            logger.warning(f"  Error GetPlayerSummaries: {e}")

    return public_players


# ── Fuente 1: BFS de amigos ───────────────────────────────────────────────────

def collect_friends_bfs(root_steamid: str, max_depth: int = 2) -> list[str]:
    """
    BFS desde el usuario raíz hasta max_depth niveles.
    Devuelve lista de steamids únicos con perfil público.
    """
    logger.info(f"[Fuente 1] BFS de amigos desde {root_steamid} (profundidad {max_depth})...")
    visited = {root_steamid}
    current_level = [root_steamid]

    for depth in range(1, max_depth + 1):
        next_level = []
        logger.info(f"  Nivel {depth}: expandiendo {len(current_level)} nodos...")

        for steamid in current_level:
            friends = get_friends(steamid)
            new = [f for f in friends if f not in visited]
            visited.update(new)
            next_level.extend(new)
            time.sleep(0.3)

        # Filtrar solo públicos
        profiles = get_player_summaries(next_level)
        public_ids = [p["steamid"] for p in profiles]
        logger.info(f"  Nivel {depth}: {len(next_level)} encontrados, {len(public_ids)} públicos.")
        current_level = public_ids

    # Excluir el nodo raíz de la lista (se carga por separado)
    result = [s for s in visited if s != root_steamid]
    logger.info(f"  BFS completado: {len(result)} steamids únicos.")
    return result


# ── Fuente 2: Miembros de grupo de Steam ─────────────────────────────────────

def collect_group_members(group_name: str) -> list[str]:
    """
    Obtiene todos los steamids de un grupo público de Steam
    paginando el XML hasta agotar los miembros.
    """
    logger.info(f"[Fuente 2] Obteniendo miembros del grupo '{group_name}'...")
    all_steamids = []
    page = 1

    while True:
        try:
            url = GROUP_XML_URL.format(group=group_name, page=page)
            r = requests.get(url, timeout=15)
            r.raise_for_status()

            root = ET.fromstring(r.text)
            members = root.findall(".//steamID64")

            if not members:
                break

            batch = [m.text for m in members if m.text]
            all_steamids.extend(batch)

            total = root.findtext("memberCount", "?")
            logger.info(f"  Página {page}: {len(batch)} miembros, total acumulado: {len(all_steamids)}/{total}")

            # Verificar si hay más páginas
            next_page = root.findtext("nextPageLink")
            if not next_page or len(batch) == 0:
                break

            page += 1
            time.sleep(1.0)

        except (requests.RequestException, ET.ParseError) as e:
            logger.warning(f"  Error en página {page}: {e}")
            break

    logger.info(f"  Grupo '{group_name}': {len(all_steamids)} steamids obtenidos.")
    return all_steamids


# ── Recolector principal ──────────────────────────────────────────────────────

def collect_all_steamids(root_steamid: str) -> list[str]:
    """
    Recolecta steamids desde todas las fuentes configuradas.
    Usa progreso guardado para continuar si se interrumpe.
    """
    progress = load_progress()
    collected_set = set(progress["collected"])
    sources_done = set(progress["sources_done"])

    # ── Fuente 1: BFS amigos ─────────────────────────────────────────────────
    if "bfs_friends" not in sources_done:
        logger.info("\n── Fuente 1: BFS de amigos ──")
        friends = collect_friends_bfs(root_steamid, max_depth=2)
        # Incluir el usuario raíz
        all_from_friends = list({root_steamid} | set(friends))
        new = [s for s in all_from_friends if s not in collected_set]
        collected_set.update(new)
        sources_done.add("bfs_friends")
        progress["collected"] = list(collected_set)
        progress["sources_done"] = list(sources_done)
        save_progress(progress)
        logger.info(f"  Fuente 1 completada: {len(new)} nuevos, total: {len(collected_set)}")

    # ── Fuente 2: Grupo steam ────────────────────────────────────────────────
    if "group_steam" not in sources_done:
        logger.info("\n── Fuente 2: Grupo 'steam' ──")
        group_members = collect_group_members("steam")
        new = [s for s in group_members if s not in collected_set]
        collected_set.update(new)
        sources_done.add("group_steam")
        progress["collected"] = list(collected_set)
        progress["sources_done"] = list(sources_done)
        save_progress(progress)
        logger.info(f"  Fuente 2 completada: {len(new)} nuevos, total: {len(collected_set)}")

    logger.info(f"\nTotal steamids recolectados: {len(collected_set)}")
    return list(collected_set)