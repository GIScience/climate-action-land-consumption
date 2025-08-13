from climatoology.base.info import Concern, _Info, PluginAuthor, generate_plugin_info

import importlib.metadata
from semver import Version
from pathlib import Path
import logging

from land_consumption.input import ComputeInput

log = logging.getLogger(__name__)


def get_info() -> _Info:
    info = generate_plugin_info(
        name='Land Consumption',
        icon=Path('resources/info/icon.jpeg'),
        authors=[
            PluginAuthor(
                name='Charles R.S. Hatfield',
                affiliation='HeiGIT gGmbH',
                website='https://heigit.org/heigit-team/',
            ),
            PluginAuthor(
                name='Emily C. Wilke',
                affiliation='HeiGIT gGmbH',
                website='https://heigit.org/heigit-team/',
            ),
            PluginAuthor(
                name='Moritz Schott',
                affiliation='HeiGIT gGmbH',
                website='https://heigit.org/heigit-team/',
            ),
            PluginAuthor(
                name='Levi Szamek',
                affiliation='HeiGIT gGmbH',
                website='https://heigit.org/heigit-team/',
            ),
            PluginAuthor(
                name='Mohammed Rizwan Khan',
                affiliation='HeiGIT gGmbH',
                website='https://heigit.org/heigit-team/',
            ),
            PluginAuthor(
                name='Sebastian Block',
                affiliation='HeiGIT gGmbH',
                website='https://heigit.org/heigit-team/',
            ),
        ],
        version=Version.parse(importlib.metadata.version('land-consumption')),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser='Estimate the proportion and type of land consumption.',
        purpose=Path('resources/info/purpose.md'),
        methodology=Path('resources/info/methodology.md'),
        sources=Path('resources/info/sources.bib'),
        demo_input_parameters=ComputeInput(),
    )
    log.info(f'Return info {info.model_dump()}')
    return info
