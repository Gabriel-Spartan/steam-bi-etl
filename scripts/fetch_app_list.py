import os
import requests
from dotenv import load_dotenv

def fetch_app_list():
    load_dotenv()
    key = os.getenv("STEAM_API_KEY")
    if not key:
        raise RuntimeError("STEAM_API_KEY no está definido en .env")

    url = "https://api.steampowered.com/IStoreService/GetAppList/v1/"
    params = {
        "key": key,
        "include_games": "true",
        "include_dlc": "false",
        "include_software": "false",
        "include_videos": "false",
        "include_hardware": "false",
        "max_results": 50000,
        "format": "json"
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    # Estructura típica: response -> apps
    apps = data.get("response", {}).get("apps", [])
    print(f"Apps recibidas: {len(apps)}")

    # muestra ejemplo
    i = 1
    for a in apps[:100]:
        print(f"{i}. {a}")
        i += 1

    return data

if __name__ == "__main__":
    fetch_app_list()