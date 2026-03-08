import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def main():
    # Carga variables desde .env (DATABASE_URL)
    load_dotenv()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL no está definido en el archivo .env")

    # Crea el engine de SQLAlchemy
    engine = create_engine(db_url, future=True)

    # Prueba conexión
    with engine.connect() as conn:
        now = conn.execute(text("SELECT now();")).scalar_one()
        user = conn.execute(text("SELECT current_user;")).scalar_one()
        db = conn.execute(text("SELECT current_database();")).scalar_one()

    print("✅ Conexión OK")
    print("Usuario:", user)
    print("Base de datos:", db)
    print("Hora del servidor:", now)

if __name__ == "__main__":
    main()