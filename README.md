# Steam BI ETL

Sistema de extracción, integración y análisis de datos de Steam orientado a construir una primera solución de **Business Intelligence (BI)** para apoyar la toma de decisiones sobre videojuegos, comportamiento de usuarios y métricas de engagement.

---

## Objetivo del proyecto

Este proyecto busca transformar datos obtenidos desde la **Steam Web API oficial** en una estructura preparada para análisis tipo **OLAP**, permitiendo construir dashboards, comparaciones entre juegos y análisis exploratorios sobre:

- Popularidad de juegos
- Actividad reciente de usuarios
- Composición de bibliotecas
- Progreso y dificultad de logros
- Eventos y noticias por juego
- Comportamiento por sistema operativo o plataforma de uso

---

## Alcance de esta primera versión

La primera versión del sistema se enfoca en el uso de la **Steam Web API oficial**, especialmente en estos grupos de datos:

- **Catálogo de juegos**
- **Perfiles de usuario** (anonimizados para análisis)
- **Juegos jugados recientemente**
- **Biblioteca de juegos del usuario**
- **Logros y porcentajes globales de desbloqueo**
- **Jugadores concurrentes por juego**
- **Noticias o eventos de un juego**

Esta versión no busca todavía cubrir toda la información posible de Steam, sino establecer una base sólida, reproducible y escalable para futuras integraciones.

---

## Enfoque de BI

El sistema sigue una lógica de trabajo en capas:

1. **Extracción** de datos desde endpoints oficiales de Steam
2. **Transformación** y limpieza de datos
3. **Carga** en PostgreSQL
4. **Modelado dimensional** con tablas de hechos y dimensiones
5. **Preparación** para dashboards y análisis comparativos

---

## Casos de análisis que permite

Entre los análisis que se pueden construir están:

- Comparación entre juegos del mismo género o nicho
- Seguimiento de popularidad por cantidad de jugadores concurrentes
- Análisis de progreso global mediante logros
- Distribución de tiempo de juego por sistema operativo
- Análisis agregado de actividad reciente de usuarios
- Identificación de logros comunes, raros y de dificultad media
- Estudio de evolución por noticias o eventos del juego

---

## Consideraciones de privacidad

El sistema contempla anonimización para cualquier análisis relacionado con usuarios. Por ello:

- No se deben publicar nombres reales
- No se deben publicar nicknames
- No se deben publicar URLs de perfil
- No se deben publicar avatares
- No se deben publicar ubicaciones exactas
- Los datos temporales deben agregarse por fecha o franja horaria

En el modelo analítico se recomienda trabajar con identificadores internos o hashes en lugar del `steamid` real.

---

## Tecnologías utilizadas

- **Python 3**
- **PostgreSQL**
- **SQLAlchemy**
- **Requests**
- **python-dotenv**
- **Pandas**
- **Tenacity**
- **Git / GitHub**
- **VS Code** (recomendado)

---

## Estructura del proyecto
```text
steam-bi-etl/
├── .env
├── .gitignore
├── README.md
├── requirements.txt
├── scripts/
│   ├── test_db.py
│   ├── steam_supported_api_list.py
│   ├── fetch_app_list.py
│   └── ...
└── src/
    ├── __init__.py
    ├── config.py
    ├── db.py
    └── ...
```

---

## Requisitos previos

Antes de ejecutar el proyecto debes tener instalado:

- Python 3.10 o superior
- pip
- PostgreSQL
- Git

En Linux también se recomienda tener activado PostgreSQL como servicio.

---

## Instalación del proyecto

### 1. Clonar o crear el proyecto

Si ya tienes la carpeta creada:
```bash
cd steam-bi-etl
```

Si vas a clonar desde GitHub:
```bash
git clone https://github.com/TU_USUARIO/steam-bi-etl.git
cd steam-bi-etl
```

### 2. Crear el entorno virtual
```bash
python3 -m venv .venv
```

### 3. Activar el entorno virtual

**En Linux / macOS:**
```bash
source .venv/bin/activate
```

**En Windows (PowerShell):**
```powershell
.venv\Scripts\Activate.ps1
```

### 4. Actualizar pip
```bash
python -m pip install --upgrade pip
```

### 5. Instalar dependencias
```bash
pip install -r requirements.txt
```

Si todavía no tienes el archivo `requirements.txt`, puedes instalar manualmente:
```bash
pip install requests python-dotenv tenacity sqlalchemy psycopg2-binary pandas
```

Y luego generar el archivo:
```bash
pip freeze > requirements.txt
```

---

## Configuración de variables de entorno

Crea un archivo `.env` en la raíz del proyecto con una estructura como esta:
```env
DATABASE_URL=postgresql+psycopg2://steam_user:TU_PASSWORD@localhost:5432/steam_bi
STEAM_API_KEY=TU_STEAM_API_KEY
STEAM_ID64=TU_STEAM_ID64
GAME_ID=1030300
STEAM_COUNTRY=EC
STEAM_LANG=spanish
```

### Descripción de variables

- `DATABASE_URL`: cadena de conexión a PostgreSQL
- `STEAM_API_KEY`: API key oficial de Steam
- `STEAM_ID64`: steamid del usuario de prueba
- `GAME_ID`: appid del juego de prueba
- `STEAM_COUNTRY`: país para consultas relacionadas a tienda
- `STEAM_LANG`: idioma preferido

> **Importante:** nunca subas tu archivo `.env` al repositorio.

---

## Configuración de PostgreSQL

### 1. Crear la base de datos

Desde PostgreSQL:
```sql
CREATE DATABASE steam_bi;
```

### 2. Crear usuario (opcional recomendado)
```sql
CREATE USER steam_user WITH PASSWORD 'TU_PASSWORD';
GRANT ALL PRIVILEGES ON DATABASE steam_bi TO steam_user;
```

### 3. Verificar conexión

Puedes ejecutar el script de prueba:
```bash
python scripts/test_db.py
```

Si todo está bien, deberías ver algo como:
```text
✅ Conexión OK
Usuario: steam_user
Base de datos: steam_bi
Hora del servidor: ...
```

---

## Cómo levantar el sistema

Por ahora este proyecto funciona principalmente mediante scripts de prueba y extracción.

### 1. Activar el entorno virtual
```bash
source .venv/bin/activate
```

### 2. Ejecutar prueba de conexión a base de datos
```bash
python scripts/test_db.py
```

### 3. Probar los endpoints disponibles
```bash
python scripts/steam_supported_api_list.py
```

### 4. Obtener el catálogo base de apps
```bash
python scripts/fetch_app_list.py
```