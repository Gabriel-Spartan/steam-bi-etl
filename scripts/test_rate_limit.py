# scripts/test_rate_limit.py
"""
Test de rate limit sostenido para GetOwnedGames.
Prueba distintas velocidades durante 5 minutos cada una.
"""
import sys
import time
import requests
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.config import get_settings

settings = get_settings()

# Steamids públicos para el test
TEST_STEAMIDS = [
    "76561198008978917",
    "76561198195382103",
    "76561198389914633",
    "76561199529564302",
    "76561199682272776",
    "76561199070045676",
]

def test_rate(delay: float, duration_seconds: int = 120) -> dict:
    """
    Envía peticiones con el delay dado durante duration_seconds.
    Devuelve estadísticas de éxito/error.
    """
    results = {"ok": 0, "rate_limit": 0, "error": 0, "total": 0}
    start = time.time()
    i = 0

    print(f"\n{'='*50}")
    print(f"Testeando delay={delay}s durante {duration_seconds}s...")
    print(f"{'='*50}")

    while time.time() - start < duration_seconds:
        steamid = TEST_STEAMIDS[i % len(TEST_STEAMIDS)]
        try:
            r = requests.get(
                "https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/",
                params={"key": settings.steam_api_key, "steamid": steamid},
                timeout=15,
            )
            results["total"] += 1

            if r.status_code == 200:
                results["ok"] += 1
            elif r.status_code == 420:
                results["rate_limit"] += 1
                print(f"  ⚠️  420 en petición {results['total']}, esperando 30s...")
                time.sleep(30)
            else:
                results["error"] += 1
                print(f"  ❌ {r.status_code} en petición {results['total']}")

            if results["total"] % 20 == 0:
                elapsed = time.time() - start
                rate = results["total"] / elapsed * 60
                print(
                    f"  [{results['total']} req | {elapsed:.0f}s] "
                    f"OK={results['ok']} 420={results['rate_limit']} "
                    f"rate={rate:.1f}/min"
                )

        except requests.RequestException as e:
            results["error"] += 1
            print(f"  Error: {e}")

        i += 1
        time.sleep(delay)

    elapsed = time.time() - start
    rate = results["total"] / elapsed * 60
    print(f"\nResultado delay={delay}s:")
    print(f"  Total: {results['total']} | OK: {results['ok']} | 420: {results['rate_limit']}")
    print(f"  Rate: {rate:.1f} req/min | Éxito: {results['ok']/results['total']*100:.1f}%")
    return results


if __name__ == "__main__":
    # El collect ya está usando ~46 req/min con delay=1.0
    # Probamos delays más agresivos para ver dónde está el límite real
    
    delays_to_test = [0.8, 0.6, 0.5]
    
    print("IMPORTANTE: El collect_user_libraries ya está corriendo.")
    print("Este test COMPARTE el rate limit con ese proceso.")
    print("Si aparecen 420s en el collect, detén este test.")
    print("\nEsperando 60s antes de empezar para no interferir...")
    time.sleep(60)

    results = {}
    for delay in delays_to_test:
        results[delay] = test_rate(delay, duration_seconds=120)
        
        if results[delay]["rate_limit"] > 0:
            print(f"\n⛔ delay={delay}s causó rate limit. Deteniendo tests.")
            break
        
        print(f"\n✅ delay={delay}s es seguro. Esperando 60s antes del siguiente...")
        time.sleep(60)

    print("\n" + "="*50)
    print("RESUMEN:")
    for delay, r in results.items():
        status = "✅ seguro" if r["rate_limit"] == 0 else "⛔ rate limit"
        print(f"  delay={delay}s: {status} ({r['ok']}/{r['total']} OK)")