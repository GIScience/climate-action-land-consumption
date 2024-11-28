import logging
from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd
import shapely
from climatoology.base.baseoperator import ComputationResources, BaseOperator, _Artifact, AoiProperties
from climatoology.base.info import generate_plugin_info, _Info, PluginAuthor, Concern
from climatoology.utility.api import LulcUtility
from ohsome import OhsomeClient

from land_consumption.artifact import build_table_artifact
from land_consumption.calculation import calculate_land_consumption
from land_consumption.input import ComputeInput
from land_consumption.utils import calculate_area, fetch_osm_area, get_ohsome_filter, LandUseCategory

log = logging.getLogger(__name__)


class LandConsumption(BaseOperator[ComputeInput]):
    def __init__(self, lulc_utility: LulcUtility):
        super().__init__()
        self.lulc_utility = lulc_utility
        self.ohsome = OhsomeClient(user_agent='CA Plugin Land Consumption')
        log.debug('Initialised Land consumption Operator')

    def info(self) -> _Info:
        info = generate_plugin_info(
            name='Land Consumption',
            icon=Path('resources/info/icon.jpeg'),
            authors=[
                PluginAuthor(
                    name='Charles Hatfield',
                    affiliation='HeiGIT gGmbH',
                    website='https://heigit.org/heigit-team/',
                ),
                PluginAuthor(
                    name='Emily Wilke',
                    affiliation='HeiGIT gGmbH',
                    website='https://heigit.org/heigit-team/',
                ),
                PluginAuthor(
                    name='Moritz Schott',
                    affiliation='HeiGIT gGmbH',
                    website='https://heigit.org/heigit-team/',
                ),
            ],
            version='dummy',
            concerns=[Concern.CLIMATE_ACTION__GHG_EMISSION],
            purpose=Path('resources/info/purpose.md'),
            methodology=Path('resources/info/methodology.md'),
            sources=Path('resources/info/sources.bib'),
        )
        log.info(f'Return info {info.model_dump()}')

        return info

    def compute(
        self,
        resources: ComputationResources,
        aoi: shapely.MultiPolygon,
        aoi_properties: AoiProperties,
        params: ComputeInput,
    ) -> List[_Artifact]:
        log.info(f'Handling compute request: {params.model_dump()} in context: {resources}')
        aoi_geom = aoi
        building_area = fetch_osm_area(
            aoi=aoi_geom, osm_filter=get_ohsome_filter(LandUseCategory.BUILDINGS), ohsome=self.ohsome
        )

        parking_area = fetch_osm_area(
            aoi=aoi_geom, osm_filter=get_ohsome_filter(LandUseCategory.PARKING_LOTS), ohsome=self.ohsome
        )

        aoi_area = calculate_area(gpd.GeoSeries(data=[aoi], crs=4326))

        land_consumption_df = calculate_land_consumption(
            aoi_area=aoi_area, building_area=building_area, parking_area=parking_area
        )
        land_consumption_table = LandConsumption.get_table(land_consumption_df)

        table_artifact = build_table_artifact(data=land_consumption_table, resources=resources)
        artifacts = [table_artifact]
        log.debug(f'Returning {len(artifacts)} artifacts.')

        return artifacts

    @staticmethod
    def get_table(land_consumption_df: pd.DataFrame) -> pd.DataFrame:
        log.debug('Creating table artifact for land consumption data.')

        total_land_consumed = land_consumption_df['% Land Consumed by known classes'].sum()
        total_land_area = land_consumption_df['Total Land Area [ha]'].sum()
        total_row = pd.DataFrame(
            {
                'Land Use': ['Total'],
                'Total Land Area [ha]': [total_land_area],
                '% Land Area': [100.0],
                '% Land Consumed by known classes': [total_land_consumed],
            }
        )

        land_consumption_table = pd.concat([land_consumption_df, total_row], ignore_index=True)

        return land_consumption_table
