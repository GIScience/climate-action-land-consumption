from enum import Enum
from pydantic_settings import BaseSettings, SettingsConfigDict


class Backend(Enum):
    OHSOME_PY = 'OHSOME_PY'
    ICEBERG = 'ICEBERG'
    ICEBERG_DUCKDB = 'ICEBERG_DUCKDB'


class Settings(BaseSettings):
    ohsome_iceberg_uri: str
    ohsome_minio_endpoint: str
    ohsome_minio_access_key: str
    ohsome_minio_access_key_id: str
    ohsome_minio_region: str = 'eu-central-1'

    backend: Backend = Backend.OHSOME_PY

    model_config = SettingsConfigDict(env_file='.env')  # dead: disable
