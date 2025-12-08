import logging.config

import duckdb
from climatoology.app.plugin import start_plugin
from ohsome import OhsomeClient
from pyiceberg.catalog.rest import RestCatalog

from land_consumption.core.operator_worker import LandConsumption
from land_consumption.core.settings import Backend, Settings

log = logging.getLogger(__name__)


def init_plugin() -> int:
    """Function to start the plugin within the architecture.

    Please adjust the class reference to the class you created above. Apart from that **DO NOT TOUCH**.

    :return:
    """
    settings = Settings()

    # TODO: refactor later: make optional input optional and only initialise what is needed
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

    con = duckdb.connect(
        # config={
        #     'threads': 10,
        #     'max_memory': '8GB',
        #     # 'enable_object_cache': True
        # }
    )
    query = f"""
        INSTALL spatial;
        INSTALL httpfs;
        LOAD spatial;
        LOAD httpfs;

        DROP SECRET IF EXISTS "__default_s3";
        CREATE SECRET (
            TYPE S3,
            KEY_ID '{settings.ohsome_minio_access_key_id}',
            SECRET '{settings.ohsome_minio_access_key}',
            REGION 'eu-central-1',
            endpoint 'hot.storage.heigit.org',
            use_ssl true,
            url_style 'path'
        );
    """
    con.sql(query)

    match settings.backend:
        case Backend.OHSOME_PY:
            data_backend = OhsomeClient(user_agent='Land-Consumption Plugin')
        case Backend.ICEBERG:
            data_backend = ohsome_catalog
        case Backend.ICEBERG_DUCKDB:
            data_backend = (ohsome_catalog, con)
        case _:
            raise NotImplementedError(f'Backend {settings.backend} not implemented.')

    operator = LandConsumption(data_connection=data_backend)

    log.info(f'Starting plugin: {operator.info().name}')
    return start_plugin(operator=operator)


if __name__ == '__main__':
    exit_code = init_plugin()
    log.info(f'Plugin exited with code {exit_code}')
