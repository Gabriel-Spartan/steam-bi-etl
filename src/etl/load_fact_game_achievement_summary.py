# src/etl/load_fact_game_achievement_summary.py
"""
Carga fact_game_achievement_summary calculando métricas
desde fact_achievement_global y dim_achievement.

NO llama a ninguna API. Es una tabla calculada 100% desde
datos ya cargados en la base de datos.

Se ejecuta DESPUÉS de:
  - load_dim_achievement
  - load_fact_achievement_global
"""
import logging

from sqlalchemy import text

from src.db import get_session, start_etl_run, finish_etl_run

logger = logging.getLogger(__name__)

CHECKPOINT_EVERY = 500


# ── SQL ───────────────────────────────────────────────────────────────────────

SQL_GET_MAX_DATE_KEY = text("""
    SELECT MAX(date_key) as d FROM fact_achievement_global
""")

SQL_GET_GAMES_WITH_GLOBAL = text("""
    SELECT DISTINCT game_key
    FROM fact_achievement_global
    WHERE date_key = (SELECT MAX(date_key) FROM fact_achievement_global)
""")

SQL_GET_ACHIEVEMENTS_FOR_GAME = text("""
    SELECT
        a.achievement_api_name,
        f.global_unlock_percent
    FROM fact_achievement_global f
    INNER JOIN dim_achievement a ON a.achievement_key = f.achievement_key
    WHERE f.game_key = :game_key
      AND f.date_key = (SELECT MAX(date_key) FROM fact_achievement_global)
      AND a.is_active = 1
    ORDER BY f.global_unlock_percent DESC
""")

SQL_MERGE = text("""
    MERGE fact_game_achievement_summary AS target
    USING (VALUES (
        :game_key, :date_key,
        :achievement_count_total,
        :most_common_achievement_name, :most_common_percent,
        :rarest_achievement_name,      :rarest_percent,
        :closest_25_name,              :closest_25_percent,
        :closest_50_name,              :closest_50_percent,
        :closest_75_name,              :closest_75_percent,
        :share_under_5_percent,
        :share_5_to_25_percent,
        :share_25_to_50_percent,
        :share_50_to_75_percent,
        :share_over_75_percent,
        :etl_run_id
    )) AS source (
        game_key, date_key,
        achievement_count_total,
        most_common_achievement_name, most_common_percent,
        rarest_achievement_name,      rarest_percent,
        closest_25_name,              closest_25_percent,
        closest_50_name,              closest_50_percent,
        closest_75_name,              closest_75_percent,
        share_under_5_percent,
        share_5_to_25_percent,
        share_25_to_50_percent,
        share_50_to_75_percent,
        share_over_75_percent,
        etl_run_id
    )
    ON target.game_key = source.game_key
   AND target.date_key = source.date_key
    WHEN MATCHED THEN
        UPDATE SET
            achievement_count_total      = source.achievement_count_total,
            most_common_achievement_name = source.most_common_achievement_name,
            most_common_percent          = source.most_common_percent,
            rarest_achievement_name      = source.rarest_achievement_name,
            rarest_percent               = source.rarest_percent,
            closest_25_name              = source.closest_25_name,
            closest_25_percent           = source.closest_25_percent,
            closest_50_name              = source.closest_50_name,
            closest_50_percent           = source.closest_50_percent,
            closest_75_name              = source.closest_75_name,
            closest_75_percent           = source.closest_75_percent,
            share_under_5_percent        = source.share_under_5_percent,
            share_5_to_25_percent        = source.share_5_to_25_percent,
            share_25_to_50_percent       = source.share_25_to_50_percent,
            share_50_to_75_percent       = source.share_50_to_75_percent,
            share_over_75_percent        = source.share_over_75_percent,
            etl_run_id                   = source.etl_run_id
    WHEN NOT MATCHED THEN
        INSERT (
            game_key, date_key,
            achievement_count_total,
            most_common_achievement_name, most_common_percent,
            rarest_achievement_name,      rarest_percent,
            closest_25_name,              closest_25_percent,
            closest_50_name,              closest_50_percent,
            closest_75_name,              closest_75_percent,
            share_under_5_percent,
            share_5_to_25_percent,
            share_25_to_50_percent,
            share_50_to_75_percent,
            share_over_75_percent,
            etl_run_id
        )
        VALUES (
            source.game_key, source.date_key,
            source.achievement_count_total,
            source.most_common_achievement_name, source.most_common_percent,
            source.rarest_achievement_name,      source.rarest_percent,
            source.closest_25_name,              source.closest_25_percent,
            source.closest_50_name,              source.closest_50_percent,
            source.closest_75_name,              source.closest_75_percent,
            source.share_under_5_percent,
            source.share_5_to_25_percent,
            source.share_25_to_50_percent,
            source.share_50_to_75_percent,
            source.share_over_75_percent,
            source.etl_run_id
        );
""")


# ── Cálculos ──────────────────────────────────────────────────────────────────

def find_closest(achievements: list, target_pct: float):
    if not achievements:
        return None, None
    closest = min(achievements, key=lambda x: abs(x[1] - target_pct))
    return closest[0], closest[1]


def calculate_summary(achievements: list, game_key: int, date_key: int, run_id: int) -> dict:
    total = len(achievements)
    most_common_name, most_common_pct = achievements[0]  if achievements else (None, None)
    rarest_name,      rarest_pct      = achievements[-1] if achievements else (None, None)

    closest_25_name, closest_25_pct = find_closest(achievements, 25.0)
    closest_50_name, closest_50_pct = find_closest(achievements, 50.0)
    closest_75_name, closest_75_pct = find_closest(achievements, 75.0)

    under_5  = sum(1 for _, p in achievements if p < 5)
    b_5_25   = sum(1 for _, p in achievements if 5  <= p < 25)
    b_25_50  = sum(1 for _, p in achievements if 25 <= p < 50)
    b_50_75  = sum(1 for _, p in achievements if 50 <= p < 75)
    over_75  = sum(1 for _, p in achievements if p >= 75)

    def pct_share(n):
        return round(n / total * 100, 2) if total > 0 else 0.0

    return {
        "game_key":                    game_key,
        "date_key":                    date_key,
        "achievement_count_total":     total,
        "most_common_achievement_name": most_common_name,
        "most_common_percent":         round(most_common_pct, 2) if most_common_pct else None,
        "rarest_achievement_name":     rarest_name,
        "rarest_percent":              round(rarest_pct, 2) if rarest_pct else None,
        "closest_25_name":             closest_25_name,
        "closest_25_percent":          round(closest_25_pct, 2) if closest_25_pct else None,
        "closest_50_name":             closest_50_name,
        "closest_50_percent":          round(closest_50_pct, 2) if closest_50_pct else None,
        "closest_75_name":             closest_75_name,
        "closest_75_percent":          round(closest_75_pct, 2) if closest_75_pct else None,
        "share_under_5_percent":       pct_share(under_5),
        "share_5_to_25_percent":       pct_share(b_5_25),
        "share_25_to_50_percent":      pct_share(b_25_50),
        "share_50_to_75_percent":      pct_share(b_50_75),
        "share_over_75_percent":       pct_share(over_75),
        "etl_run_id":                  run_id,
    }


# ── Load ──────────────────────────────────────────────────────────────────────

def load() -> None:
    run_id   = start_etl_run("load_fact_game_achievement_summary")
    inserted = 0
    skipped  = 0

    try:
        # Obtener date_key máximo disponible en fact_achievement_global
        with get_session() as session:
            max_date_row = session.execute(SQL_GET_MAX_DATE_KEY).fetchone()

        if not max_date_row or not max_date_row.d:
            logger.warning("fact_achievement_global está vacía. Ejecuta load_fact_achievement_global primero.")
            finish_etl_run(run_id, status="failed",
                           error_message="fact_achievement_global vacía")
            return

        date_key = int(max_date_row.d)
        logger.info(f"Usando date_key: {date_key}")

        # Obtener juegos con datos en fact_achievement_global
        with get_session() as session:
            game_rows = session.execute(SQL_GET_GAMES_WITH_GLOBAL).fetchall()

        total = len(game_rows)
        logger.info(f"  {total:,} juegos a procesar.")

        if total == 0:
            logger.warning("No hay juegos con datos en fact_achievement_global.")
            finish_etl_run(run_id, status="failed",
                           error_message="fact_achievement_global vacía")
            return

        batch = []

        for i, row in enumerate(game_rows, 1):
            game_key = row.game_key

            with get_session() as session:
                ach_rows = session.execute(
                    SQL_GET_ACHIEVEMENTS_FOR_GAME,
                    {"game_key": game_key}
                ).fetchall()

            if not ach_rows:
                skipped += 1
                continue

            achievements = [
                (r.achievement_api_name, float(r.global_unlock_percent))
                for r in ach_rows
            ]

            summary = calculate_summary(achievements, game_key, date_key, run_id)
            batch.append(summary)

            if len(batch) >= 200:
                with get_session() as session:
                    session.execute(SQL_MERGE, batch)
                inserted += len(batch)
                batch = []

            if i % CHECKPOINT_EVERY == 0:
                logger.info(f"  [{i:,}/{total:,}] insertados={inserted:,} skip={skipped:,}")

        if batch:
            with get_session() as session:
                session.execute(SQL_MERGE, batch)
            inserted += len(batch)

        finish_etl_run(run_id, status="success",
                       rows_inserted=inserted, rows_skipped=skipped)
        logger.info(
            f"✅ fact_game_achievement_summary completado:"
            f"\n  Resúmenes insertados: {inserted:,}"
            f"\n  Juegos sin datos:     {skipped:,}"
        )

    except Exception as e:
        finish_etl_run(run_id, status="failed", error_message=str(e))
        raise


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(message)s")
    load()