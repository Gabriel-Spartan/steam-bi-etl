# src/etl/load_fact_game_price_period.py
"""
Carga fact_game_price_period calculando períodos de precio
desde fact_game_price_snapshot.

Lógica:
  - Agrupa snapshots por game_key + country_key + currency_key
  - Detecta cambios de precio entre snapshots consecutivos
  - Crea un período nuevo cuando el precio cambia
  - Cierra el período anterior (valid_to_date) cuando hay cambio

NO llama a ninguna API. Es 100% calculado desde datos ya en la BD.
Se ejecuta DESPUÉS de load_fact_game_price_snapshot.
"""
import logging
from datetime import date, datetime

from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run

logger = logging.getLogger(__name__)

BATCH_SIZE       = 200
CHECKPOINT_EVERY = 1000


# ── SQL ───────────────────────────────────────────────────────────────────────

SQL_GET_GAME_COUNTRY_PAIRS = text("""
    SELECT DISTINCT game_key, country_key, currency_key
    FROM fact_game_price_snapshot
    ORDER BY game_key, country_key, currency_key
""")

SQL_GET_SNAPSHOTS_FOR_GAME = text("""
    SELECT
        date_key,
        captured_at,
        initial_price,
        final_price,
        discount_percent
    FROM fact_game_price_snapshot
    WHERE game_key    = :game_key
      AND country_key = :country_key
      AND currency_key = :currency_key
    ORDER BY captured_at ASC
""")

SQL_GET_EXISTING_PERIOD = text("""
    SELECT valid_from_date, initial_price, final_price, discount_percent
    FROM fact_game_price_period
    WHERE game_key     = :game_key
      AND country_key  = :country_key
      AND currency_key = :currency_key
      AND valid_to_date IS NULL
""")

SQL_CLOSE_PERIOD = text("""
    UPDATE fact_game_price_period
    SET valid_to_date = :valid_to_date
    WHERE game_key     = :game_key
      AND country_key  = :country_key
      AND currency_key = :currency_key
      AND valid_to_date IS NULL
""")

SQL_INSERT_PERIOD = text("""
    INSERT INTO fact_game_price_period (
        game_key, country_key, currency_key,
        valid_from_date, valid_to_date,
        initial_price, final_price, discount_percent,
        created_at, etl_run_id
    )
    VALUES (
        :game_key, :country_key, :currency_key,
        :valid_from_date, :valid_to_date,
        :initial_price, :final_price, :discount_percent,
        GETDATE(), :etl_run_id
    )
""")

SQL_COUNT_PERIODS = text("""
    SELECT COUNT(*) as n FROM fact_game_price_period
""")


# ── Helpers ───────────────────────────────────────────────────────────────────

def date_from_key(date_key: int) -> date:
    """Convierte YYYYMMDD a date."""
    s = str(date_key)
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def prices_equal(s1: dict, s2: dict) -> bool:
    """Compara si dos snapshots tienen el mismo precio."""
    return (
        s1["initial_price"]   == s2["initial_price"] and
        s1["final_price"]     == s2["final_price"] and
        s1["discount_percent"] == s2["discount_percent"]
    )


# ── Load ──────────────────────────────────────────────────────────────────────

def load() -> None:
    run_id   = start_etl_run("load_fact_game_price_period")
    inserted = 0
    updated  = 0
    skipped  = 0

    try:
        # Paso 1: obtener todos los pares game+country+currency
        logger.info("Obteniendo combinaciones de juego/país/moneda...")
        with get_session() as session:
            pairs = session.execute(SQL_GET_GAME_COUNTRY_PAIRS).fetchall()

        total = len(pairs)
        logger.info(f"  {total:,} combinaciones a procesar.")

        if total == 0:
            logger.warning(
                "  fact_game_price_snapshot está vacía. "
                "Ejecuta load_fact_game_price_snapshot primero."
            )
            finish_etl_run(run_id, status="failed",
                           error_message="fact_game_price_snapshot vacía")
            return

        # Paso 2: procesar cada combinación
        for i, pair in enumerate(pairs, 1):
            game_key    = pair.game_key
            country_key = pair.country_key
            currency_key = pair.currency_key

            # Obtener snapshots ordenados por fecha
            with get_session() as session:
                snapshots = session.execute(
                    SQL_GET_SNAPSHOTS_FOR_GAME,
                    {
                        "game_key":    game_key,
                        "country_key": country_key,
                        "currency_key": currency_key,
                    }
                ).fetchall()

            if not snapshots:
                skipped += 1
                continue

            # Convertir a lista de dicts para comparar
            snaps = [
                {
                    "date_key":        s.date_key,
                    "captured_at":     s.captured_at,
                    "initial_price":   s.initial_price,
                    "final_price":     s.final_price,
                    "discount_percent": s.discount_percent,
                }
                for s in snapshots
            ]

            # Obtener período abierto actual si existe
            with get_session() as session:
                existing = session.execute(
                    SQL_GET_EXISTING_PERIOD,
                    {
                        "game_key":    game_key,
                        "country_key": country_key,
                        "currency_key": currency_key,
                    }
                ).fetchone()

            # Detectar períodos comparando snapshots consecutivos
            periods_to_insert = []
            current_period_start = None
            current_snap         = None

            for snap in snaps:
                if current_snap is None:
                    # Primer snapshot → inicio del primer período
                    current_period_start = date_from_key(snap["date_key"])
                    current_snap         = snap
                    continue

                if not prices_equal(current_snap, snap):
                    # El precio cambió → cerrar período anterior y abrir nuevo
                    period_end = date_from_key(snap["date_key"])

                    periods_to_insert.append({
                        "game_key":        game_key,
                        "country_key":     country_key,
                        "currency_key":    currency_key,
                        "valid_from_date": current_period_start,
                        "valid_to_date":   period_end,
                        "initial_price":   current_snap["initial_price"],
                        "final_price":     current_snap["final_price"],
                        "discount_percent": current_snap["discount_percent"],
                        "etl_run_id":      run_id,
                    })

                    current_period_start = period_end
                    current_snap         = snap

            # Período abierto (el más reciente, sin valid_to_date)
            if current_snap is not None:
                if existing and prices_equal(
                    existing._asdict() if hasattr(existing, '_asdict') else {
                        "initial_price":   existing.initial_price,
                        "final_price":     existing.final_price,
                        "discount_percent": existing.discount_percent,
                    },
                    current_snap
                ):
                    # El período abierto no cambió → no hacer nada
                    skipped += 1
                else:
                    # Cerrar período abierto anterior si existe
                    if existing:
                        with get_session() as session:
                            session.execute(SQL_CLOSE_PERIOD, {
                                "game_key":     game_key,
                                "country_key":  country_key,
                                "currency_key": currency_key,
                                "valid_to_date": current_period_start,
                            })
                        updated += 1

                    # Agregar período abierto actual
                    periods_to_insert.append({
                        "game_key":        game_key,
                        "country_key":     country_key,
                        "currency_key":    currency_key,
                        "valid_from_date": current_period_start,
                        "valid_to_date":   None,
                        "initial_price":   current_snap["initial_price"],
                        "final_price":     current_snap["final_price"],
                        "discount_percent": current_snap["discount_percent"],
                        "etl_run_id":      run_id,
                    })

            # Insertar períodos calculados
            if periods_to_insert:
                with get_session() as session:
                    session.execute(SQL_INSERT_PERIOD, periods_to_insert)
                inserted += len(periods_to_insert)

            if i % CHECKPOINT_EVERY == 0:
                logger.info(
                    f"  [{i:,}/{total:,}] "
                    f"insertados={inserted:,} actualizados={updated:,} "
                    f"skip={skipped:,}"
                )

        finish_etl_run(
            run_id, status="success",
            rows_inserted=inserted,
            rows_updated=updated,
            rows_skipped=skipped,
        )
        logger.info(
            f"✅ fact_game_price_period completado:"
            f"\n  Períodos insertados:  {inserted:,}"
            f"\n  Períodos actualizados: {updated:,}"
            f"\n  Saltados:             {skipped:,}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()
