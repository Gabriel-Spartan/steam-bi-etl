import os
import requests
from dotenv import load_dotenv

def main():
    load_dotenv()
    key = os.getenv("STEAM_API_KEY")
    if not key:
        raise RuntimeError("STEAM_API_KEY no está definido en .env")

    url = "https://api.steampowered.com/ISteamWebAPIUtil/GetSupportedAPIList/v1/"
    r = requests.get(url, params={"key": key, "format": "json"}, timeout=30)
    r.raise_for_status()

    data = r.json()
    interfaces = data["apilist"]["interfaces"]

    print(f"✅ Interfaces encontradas: {len(interfaces)}\n")
    i = 1
    for iface in interfaces:
        print(f"{i}. {iface['name']}")
        i += 1
    print("\n(Se imprimieron las primeras 20. Si quieres, lo guardamos en Postgres.)")

if __name__ == "__main__":
    main()