from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    ohsome_base_url: Optional[str] = None

    model_config = SettingsConfigDict(env_file='.env')  # dead: disable
