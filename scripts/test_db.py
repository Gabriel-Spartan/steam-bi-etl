# scripts/test_db.py
import sys
import logging
from pathlib import Path

# Asegura que src/ sea importable desde scripts/
sys.path.append(str(Path(__file__).resolve().parent.parent))

logging.basicConfig(level=logging.INFO, format="%(message)s")

from src.db import check_connection

if __name__ == "__main__":
    ok = check_connection()
    sys.exit(0 if ok else 1)