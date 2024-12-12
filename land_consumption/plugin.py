import logging.config

from climatoology.app.plugin import start_plugin
from pydantic_settings import BaseSettings, SettingsConfigDict
from pyiceberg.catalog.rest import RestCatalog

from land_consumption.operator_worker import LandConsumption

log = logging.getLogger(__name__)


class Settings(BaseSettings):
    ohsome_iceberg_uri: str
    ohsome_minio_endpoint: str
    ohsome_minio_access_key: str
    ohsome_minio_access_key_id: str
    ohsome_minio_region: str = 'eu-central-1'

    model_config = SettingsConfigDict(env_file='.env')


def init_plugin() -> int:
    """Function to start the plugin within the architecture.

    Please adjust the class reference to the class you created above. Apart from that **DO NOT TOUCH**.

    :return:
    """
    settings = Settings()

    ohsome_catalog = RestCatalog(
        name='default',
        **{
            'uri': settings.ohsome_iceberg_uri,
            's3.endpoint': settings.ohsome_minio_endpoint,
            'py-io-impl': 'pyiceberg.io.pyarrow.PyArrowFileIO',
            's3.access-key-id': settings.ohsome_minio_access_key_id,
            's3.secret-access-key': settings.ohsome_minio_access_key,
            's3.region': settings.ohsome_minio_region,
        },
    )

    operator = LandConsumption(ohsome_catalog=ohsome_catalog)

    log.info(f'Starting plugin: {operator.info().name}')
    return start_plugin(operator=operator)


if __name__ == '__main__':
    exit_code = init_plugin()
    log.info(f'Plugin exited with code {exit_code}')
