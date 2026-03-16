# src/db.py
import logging
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from src.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    echo=False,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


@contextmanager
def get_session() -> Session:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def check_connection() -> bool:
    try:
        with get_session() as session:
            result = session.execute(text("SELECT @@VERSION AS version"))
            row = result.fetchone()
            logger.info(f"Conexión OK — {row.version[:80]}...")
        return True
    except Exception as e:
        logger.error(f"Error de conexión: {e}")
        return False


def start_etl_run(script_name: str) -> int:
    """Registra el inicio de una ejecución ETL y devuelve el run_id."""
    sql = text("""
        INSERT INTO etl_run_log (script_name, started_at, status)
        OUTPUT INSERTED.run_id
        VALUES (:script_name, GETDATE(), 'running')
    """)
    with get_session() as session:
        result = session.execute(sql, {"script_name": script_name})
        return result.fetchone()[0]


def finish_etl_run(
    run_id: int,
    status: str = "success",
    rows_inserted: int = 0,
    rows_updated: int = 0,
    rows_skipped: int = 0,
    error_message: str = None,
) -> None:
    """Registra el fin de una ejecución ETL con su resultado."""
    sql = text("""
        UPDATE etl_run_log
        SET finished_at   = GETDATE(),
            status        = :status,
            rows_inserted = :rows_inserted,
            rows_updated  = :rows_updated,
            rows_skipped  = :rows_skipped,
            error_message = :error_message
        WHERE run_id = :run_id
    """)
    with get_session() as session:
        session.execute(sql, {
            "run_id":        run_id,
            "status":        status,
            "rows_inserted": rows_inserted,
            "rows_updated":  rows_updated,
            "rows_skipped":  rows_skipped,
            "error_message": error_message,
        })