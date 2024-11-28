import logging.config

from climatoology.app.plugin import start_plugin
from climatoology.utility.api import LulcUtility
from pydantic_settings import BaseSettings, SettingsConfigDict

from land_consumption.operator_worker import LandConsumption

log = logging.getLogger(__name__)


class Settings(BaseSettings):
    lulc_host: str
    lulc_port: int
    lulc_path: str

    model_config = SettingsConfigDict(env_file='.env')


def init_plugin() -> None:
    """Function to start the plugin within the architecture.

    Please adjust the class reference to the class you created above. Apart from that **DO NOT TOUCH**.

    :return:
    """
    settings = Settings()

    lulc_utility = LulcUtility(
        host=settings.lulc_host,
        port=settings.lulc_port,
        path=settings.lulc_path,
    )
    operator = LandConsumption(lulc_utility=lulc_utility)

    log.info(f'Starting plugin: {operator.info().name}')
    return start_plugin(operator=operator)


if __name__ == '__main__':
    exit_code = init_plugin()
    log.info(f'Plugin exited with code {exit_code}')
