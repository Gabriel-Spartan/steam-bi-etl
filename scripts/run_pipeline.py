# scripts/run_pipeline.py
"""
Pipeline completo del DWH de Steam.
Ejecuta todos los ETLs en el orden correcto.

Uso:
    python scripts/run_pipeline.py                        # todo desde cero
    python scripts/run_pipeline.py --from dim_game        # reanudar desde un paso
    python scripts/run_pipeline.py --only dim_genre       # solo un paso
    python scripts/run_pipeline.py --skip-errors          # continuar si hay errores
    python scripts/run_pipeline.py --list                 # listar todos los pasos
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

Path("logs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/pipeline.log", encoding="utf-8"),
    ]
)

logger = logging.getLogger("pipeline")


STEPS = [
    # ── Catálogos estáticos (sin dependencias) ────────────────────────────────
    ("dim_country",      "src.etl.load_dim_country",     "load"),
    ("dim_currency",     "src.etl.load_dim_currency",    "load"),
    ("dim_language",     "src.etl.load_dim_language",    "load"),
    ("dim_time_bucket",  "src.etl.load_dim_time_bucket", "load"),
    ("dim_date",         "src.etl.load_dim_date",        "load"),

    # ── Dimensiones de juego (base masiva desde SteamSpy) ────────────────────
    ("dim_game",         "src.etl.load_dim_game",        "load"),

    # ── Dimensiones de lookup desde appdetails ────────────────────────────────
    ("dim_genre",        "src.etl.load_dim_genre",       "load"),
    ("dim_category",     "src.etl.load_dim_category",    "load"),
    ("dim_developer",    "src.etl.load_dim_developer",   "load"),
    ("dim_publisher",    "src.etl.load_dim_publisher",   "load"),

    # ── Usuarios (BFS amigos + grupo Steam) ──────────────────────────────────
    ("dim_user",         "src.etl.load_dim_user",        "load"),

    # ── Recolección de bibliotecas (proceso largo, genera caché) ─────────────
    ("collect_libraries","src.etl.collect_user_libraries","collect"),

    # ── Enriquecimiento de dim_game con appdetails (usa caché de bibliotecas) ─
    ("enrich_dim_game",  "src.etl.enrich_dim_game",      "load"),

    # ── Bridges (usan caché appdetails_top500) ────────────────────────────────
    ("bridge_game_genre",    "src.etl.load_bridge_game_genre",    "load"),
    ("bridge_game_category", "src.etl.load_bridge_game_category", "load"),
    ("bridge_game_developer","src.etl.load_bridge_game_developer","load"),
    ("bridge_game_publisher","src.etl.load_bridge_game_publisher","load"),
    ("bridge_game_language", "src.etl.load_bridge_game_language", "load"),

    # ── Dimensión de logros (depende de dim_game enriquecida) ─────────────────
    ("dim_achievement",  "src.etl.load_dim_achievement", "load"),

    # ── Facts de usuario (dependen de bibliotecas recolectadas) ──────────────
    ("fact_user_owned_game",       "src.etl.load_fact_user_owned_game",       "load"),
    ("fact_user_library_snapshot", "src.etl.load_fact_user_library_snapshot", "load"),
    ("fact_user_recent_play",      "src.etl.load_fact_user_recent_play",      "load"),

    # ── Facts de juego (dependen de dim_game enriquecida) ────────────────────
    ("fact_concurrent_players",    "src.etl.load_fact_concurrent_players_snapshot", "load"),
    ("fact_news_events",           "src.etl.load_fact_news_events",           "load"),
    ("fact_game_price_snapshot",   "src.etl.load_fact_game_price_snapshot",   "load"),
    ("fact_game_review_summary",   "src.etl.load_fact_game_review_summary",   "load"),
    ("fact_game_review_detail",    "src.etl.load_fact_game_review_detail",    "load"),

    # ── Facts de logros (dependen de dim_achievement + fact_achievement_global)
    ("fact_achievement_global",       "src.etl.load_fact_achievement_global",       "load"),
    ("fact_game_achievement_summary", "src.etl.load_fact_game_achievement_summary", "load"),
]


def run_step(name: str, module_path: str, func_name: str) -> bool:
    """Ejecuta un paso del pipeline. Devuelve True si fue exitoso."""
    import importlib
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f"▶  {name}")
        logger.info(f"{'='*60}")
        module = importlib.import_module(module_path)
        func   = getattr(module, func_name)
        func()
        logger.info(f"✅ {name} completado.\n")
        return True
    except Exception as e:
        logger.error(f"❌ {name} falló: {e}", exc_info=True)
        return False


def main():
    parser = argparse.ArgumentParser(description="Pipeline ETL Steam BI")
    parser.add_argument(
        "--from", dest="start_from", default=None,
        help="Reanudar desde este paso (ej: --from dim_game)"
    )
    parser.add_argument(
        "--only", dest="only", default=None,
        help="Ejecutar solo este paso (ej: --only dim_genre)"
    )
    parser.add_argument(
        "--skip-errors", action="store_true",
        help="Continuar aunque un paso falle"
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Listar todos los pasos disponibles"
    )
    args = parser.parse_args()

    # Listar pasos disponibles
    if args.list:
        print("\nPasos disponibles en el pipeline:\n")
        for idx, (name, module, func) in enumerate(STEPS, 1):
            print(f"  {idx:2}. {name}")
        print()
        return

    # Filtrar pasos según argumentos
    steps = STEPS
    if args.only:
        steps = [(n, m, f) for n, m, f in STEPS if n == args.only]
        if not steps:
            logger.error(f"Paso '{args.only}' no encontrado. Usa --list para ver los disponibles.")
            sys.exit(1)
    elif args.start_from:
        names = [n for n, _, _ in STEPS]
        if args.start_from not in names:
            logger.error(f"Paso '{args.start_from}' no encontrado. Usa --list para ver los disponibles.")
            sys.exit(1)
        idx   = names.index(args.start_from)
        steps = STEPS[idx:]
        logger.info(f"Reanudando desde '{args.start_from}' ({len(steps)} pasos restantes)")

    logger.info(f"Pipeline iniciado. Pasos a ejecutar: {len(steps)}")
    failed = []

    for name, module_path, func_name in steps:
        success = run_step(name, module_path, func_name)
        if not success:
            failed.append(name)
            if not args.skip_errors:
                logger.error(
                    f"\nPipeline detenido en '{name}'."
                    f"\nPara reanudar: python scripts/run_pipeline.py --from {name}"
                    f"\nPara saltar errores: python scripts/run_pipeline.py --from {name} --skip-errors"
                )
                sys.exit(1)

    if failed:
        logger.warning(f"\nPipeline completado con errores en: {', '.join(failed)}")
        sys.exit(1)
    else:
        logger.info("\n✅ Pipeline completado exitosamente.")


if __name__ == "__main__":
    main()