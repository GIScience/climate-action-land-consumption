import logging.config

from climatoology.app.plugin import start_plugin
from ohsome_py2.client import OhsomeClient

from land_consumption.core.operator_worker import LandConsumption
from land_consumption.core.settings import Settings

log = logging.getLogger(__name__)


def init_plugin() -> int:
    """Function to start the plugin within the architecture.

    Please adjust the class reference to the class you created above. Apart from that **DO NOT TOUCH**.

    :return:
    """
    settings = Settings()
    ohsome_client = OhsomeClient(
        user_agent='Land-Consumption Plugin', v2=settings.feature_flag_ohsome2, base_url=settings.ohsome_base_url
    )
    operator = LandConsumption(ohsome_client=ohsome_client)

    log.info(f'Starting plugin: {operator.info().name}')
    return start_plugin(operator=operator)


if __name__ == '__main__':
    exit_code = init_plugin()
    log.info(f'Plugin exited with code {exit_code}')
