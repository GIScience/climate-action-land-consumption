import logging
from pathlib import Path
from typing import List

import pandas as pd
from climatoology.base.operator import ComputationResources, Concern, Info, Operator, PluginAuthor, _Artifact
from climatoology.utility.api import LulcUtility

from land_consumption.artifact import build_table_artifact
from land_consumption.calculate import calculate_land_consumption
from land_consumption.input import ComputeInput

log = logging.getLogger(__name__)


class LandConsumption(Operator[ComputeInput]):
    def __init__(self, lulc_utility: LulcUtility):
        self.lulc_utility = lulc_utility
        log.debug('Initialised Land consumption Operator')

    def info(self) -> Info:
        info = Info(
            name='Land Consumption',
            icon=Path('resources/info/icon.jpeg'),
            authors=[
                PluginAuthor(
                    name='Charles Hatfield',
                    affiliation='HeiGIT gGmbH',
                    website='https://heigit.org/heigit-team/',
                ),
                PluginAuthor(
                    name='Jonas Kemmer',
                    affiliation='HeiGIT gGmbH',
                    website='https://heigit.org/heigit-team/',
                ),
            ],
            version='dummy',
            concerns=[Concern.CLIMATE_ACTION__GHG_EMISSION],
            purpose=Path('resources/info/purpose.md').read_text(),
            methodology=Path('resources/info/methodology.md').read_text(),
            sources=Path('resources/info/sources.bib'),
        )
        log.info(f'Return info {info.model_dump()}')

        return info

    def compute(self, resources: ComputationResources, params: ComputeInput) -> List[_Artifact]:
        log.info(f'Handling compute request: {params.model_dump()} in context: {resources}')

        land_consumption_df = calculate_land_consumption()
        land_consumption_table = LandConsumption.get_table(land_consumption_df)

        table_artifact = build_table_artifact(land_consumption_table, resources)
        artifacts = [
            table_artifact,
        ]
        log.debug(f'Returning {len(artifacts)} artifacts.')

        return artifacts

    @staticmethod
    def get_table(land_consumption_df: pd.DataFrame) -> pd.DataFrame:
        log.debug('Creating table artifact for land consumption data.')

        total_land_consumed = land_consumption_df['% Land Consumed'].sum()
        total_row = pd.DataFrame(
            {
                'Land Use': ['Total'],
                'Total Land Area [ha]': [100],
                '% Land Area': [100],
                '% Land Consumed': [total_land_consumed],
            }
        )

        land_consumption_table = pd.concat([land_consumption_df, total_row], ignore_index=True)

        return land_consumption_table
