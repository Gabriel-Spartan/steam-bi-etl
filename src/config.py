# src/config.py
from functools import lru_cache
from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    database_url: str
    steam_api_key: str
    steam_api_key_2: str = ""  # opcional
    steam_api_key_3: str = ""  # opcional
    steam_api_key_4: str = ""  # opcional
    steam_api_key_5: str = ""  # opcional
    steam_api_key_6: str = ""  # opcional
    steam_id64: str
    game_id: int = Field(alias="GAME_ID", default=1030300)
    steam_country: str = "EC"
    steam_lang: str = "spanish"

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "populate_by_name": True,
    }


@lru_cache()
def get_settings() -> Settings:
    return Settings()