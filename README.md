# Steam BI ETL

Sistema de extracción, transformación y carga (ETL) de datos de Steam orientado a construir una solución de **Business Intelligence (BI)** para análisis de videojuegos, comportamiento de usuarios y métricas de engagement.

---

## Objetivo del proyecto

Transformar datos de la **Steam Web API** en una estructura dimensional preparada para análisis OLAP, permitiendo construir dashboards y análisis exploratorios sobre:

- Popularidad de juegos y jugadores concurrentes
- Actividad reciente de usuarios y composición de bibliotecas
- Progreso y dificultad de logros
- Precios, descuentos y evolución de reseñas
- Noticias y eventos por juego

---

## Arquitectura
```
Steam Web API          SteamSpy API          Store API
     │                     │                     │
     └─────────────────────┴─────────────────────┘
                           │
                    src/etl/*.py
                    (29 scripts ETL)
                           │
                    SQL Server (Steam_BI)
                    (DWH dimensional)
                           │
                    Dashboards / OLAP
```

El modelo sigue un esquema estrella con dimensiones (`dim_*`), tablas puente (`bridge_*`) y hechos (`fact_*`). Incluye SCD Tipo 2 en `dim_game` y `dim_user` para rastrear cambios históricos.

---

## Tecnologías

| Tecnología | Uso |
|---|---|
| Python 3.10+ | Lenguaje principal |
| SQL Server 2022 | Base de datos del DWH |
| SQLAlchemy 2.0 | ORM y conexión a BD |
| pyodbc | Driver ODBC para SQL Server |
| pydantic-settings | Validación de variables de entorno |
| requests | Llamadas a APIs de Steam |
| Git / GitHub | Control de versiones |

---

## Requisitos previos

### Todos los sistemas operativos

- Python 3.10 o superior
- Git
- SQL Server 2022 (o SQL Server Express, gratuito)
- ODBC Driver 18 for SQL Server

### Instalar ODBC Driver 18

**Ubuntu / Debian:**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list \
  | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt update
sudo ACCEPT_EULA=Y apt install -y msodbcsql18
```

**Fedora / RHEL / CentOS:**
```bash
curl https://packages.microsoft.com/config/rhel/9/prod.repo \
  | sudo tee /etc/yum.repos.d/mssql-release.repo
sudo ACCEPT_EULA=Y dnf install -y msodbcsql18
```

**macOS:**
```bash
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew install msodbcsql18
```

**Windows:**
Descargar e instalar desde:
https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

---

## Instalación

### 1. Clonar el repositorio
```bash
git clone https://github.com/Gabriel-Spartan/steam-bi-etl.git
cd steam-bi-etl
```

### 2. Crear y activar el entorno virtual

**Linux / macOS:**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

**Windows (PowerShell):**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3. Instalar dependencias
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

---

## Configuración

### 1. Variables de entorno

Crea un archivo `.env` en la raíz del proyecto:
```dotenv
# Base de datos
DATABASE_URL=mssql+pyodbc://sa:TU_PASSWORD@localhost/Steam_BI?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes

# Steam API (requerida)
STEAM_API_KEY=TU_STEAM_API_KEY
STEAM_ID64=TU_STEAM_ID64

# Steam API keys adicionales (opcionales, mejoran el throughput)
STEAM_API_KEY_2=
STEAM_API_KEY_3=
STEAM_API_KEY_4=
STEAM_API_KEY_5=

# Configuración regional
STEAM_COUNTRY=EC
STEAM_LANG=spanish
```

Para obtener tu `STEAM_API_KEY` visita: https://steamcommunity.com/dev/apikey  
Tu `STEAM_ID64` lo puedes encontrar en: https://steamid.io

### 2. Crear la base de datos

Conéctate a SQL Server y ejecuta:
```bash
sqlcmd -S localhost -U sa -P 'TU_PASSWORD' -No \
  -i docs/architecture/ScriptSteamDataWarehouse.sql
```

### 3. Verificar la conexión
```bash
python scripts/test_db.py
```

Deberías ver:
```
✅ Conexión OK — Microsoft SQL Server 2022...
```

---

## Uso del pipeline

El pipeline completo se ejecuta con un solo comando:
```bash
python scripts/run_pipeline.py
```

### Opciones disponibles
```bash
# Ver todos los pasos disponibles
python scripts/run_pipeline.py --list

# Ejecutar desde un paso específico (útil para reanudar)
python scripts/run_pipeline.py --from dim_game

# Ejecutar solo un paso
python scripts/run_pipeline.py --only dim_genre

# Continuar aunque un paso falle
python scripts/run_pipeline.py --skip-errors
```

### Pasos del pipeline

| # | Paso | Fuente | Tiempo estimado |
|---|---|---|---|
| 1 | `dim_country` | RestCountries API | < 1 min |
| 2 | `dim_currency` | Estático | < 1 min |
| 3 | `dim_language` | Estático | < 1 min |
| 4 | `dim_time_bucket` | Estático | < 1 min |
| 5 | `dim_date` | Calculado | < 1 min |
| 6 | `dim_game` | SteamSpy API | ~2 min |
| 7-10 | `dim_genre/category/developer/publisher` | Steam appdetails | ~3 min |
| 11 | `dim_user` | Steam API + grupos | ~5 min |
| 12 | `collect_libraries` | Steam GetOwnedGames | **~6 horas** |
| 13 | `enrich_dim_game` | Steam appdetails | **~18 horas** |
| 14-18 | Bridges | Steam appdetails | ~1 min |
| 19 | `dim_achievement` | GetSchemaForGame | **~6 horas** |
| 20-22 | Facts de usuario | Caché bibliotecas | ~30 min |
| 23-27 | Facts de juego | Steam/Store API | **~10-20 horas** |
| 28-29 | Facts de logros | Steam API + calculado | ~5 horas |

> **Nota:** los tiempos largos se deben al rate limiting de Steam (~200 req/5min por API key). Con múltiples keys (`STEAM_API_KEY_2` al `_5`) el tiempo se reduce proporcionalmente. El pipeline es **idempotente**: puedes interrumpirlo y reanudarlo con `--from PASO` sin perder progreso.

---

## Estructura del proyecto
```text
steam-bi-etl/
├── .env                          # Variables de entorno (no en git)
├── .gitignore
├── README.md
├── requirements.txt
├── data/
│   └── cache/                    # Caché local de APIs (no en git)
├── docs/
│   ├── architecture/
│   │   └── DataBase.md           # Diagrama ERD del DWH
│   ├── Endpoints.md              # Referencia de endpoints de Steam
│   ├── loadingOrder.md           # Orden de carga y dependencias
│   └── tablesDB.md               # Notas de implementación
├── logs/                         # Logs del pipeline (no en git)
├── scripts/
│   ├── run_pipeline.py           # Orquestador principal
│   ├── test_db.py                # Verificación de conexión
│   ├── test_rate_limit.py        # Test de rate limits de Steam
│   ├── fetch_app_list.py         # Utilidad de diagnóstico
│   └── steam_supported_api_list.py
└── src/
    ├── config.py                 # Settings con pydantic-settings
    ├── db.py                     # Engine, sesiones, etl_run_log
    └── etl/
        ├── steam_appdetails.py   # Módulo compartido appdetails
        ├── steam_users.py        # Módulo compartido usuarios
        ├── collect_user_libraries.py  # Recolección de bibliotecas
        ├── enrich_dim_game.py    # Enriquecimiento SCD2
        ├── load_dim_*.py         # ETLs de dimensiones (10 archivos)
        ├── load_bridge_*.py      # ETLs de bridges (5 archivos)
        └── load_fact_*.py        # ETLs de hechos (10 archivos)
```

---

## Consideraciones de privacidad

El sistema anonimiza todos los datos de usuarios:

- Los `steamid` se almacenan como hash SHA-256
- No se guardan nombres, nicknames, avatares ni URLs de perfil
- Las horas se agregan en franjas (`dim_time_bucket`) sin timestamp exacto
- Las ubicaciones se reducen a código de país (`dim_country`)

---

## Cargas incrementales

Después de la carga inicial, las siguientes ejecuciones son mucho más rápidas porque cada ETL verifica el progreso guardado en `data/cache/` y solo procesa lo nuevo:
```bash
# Carga incremental completa
python scripts/run_pipeline.py

# Solo actualizar datos de usuarios
python scripts/run_pipeline.py --from collect_libraries

# Solo actualizar snapshots del día
python scripts/run_pipeline.py --only fact_concurrent_players
```

La tabla `etl_run_log` en SQL Server registra cada ejecución con timestamps, filas insertadas/actualizadas y estado, permitiendo auditoría completa del pipeline.