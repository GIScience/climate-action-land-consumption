import importlib
import logging
from pathlib import Path
from typing import List

import geopandas as gpd
import pandas as pd
import shapely
from climatoology.base.baseoperator import BaseOperator, _Artifact, AoiProperties, ComputationResources
from climatoology.base.info import Concern, _Info, PluginAuthor, generate_plugin_info
from pyiceberg.catalog.rest import RestCatalog
from semver import Version

from land_consumption.artifact import build_table_artifact
from land_consumption.calculation import calculate_land_consumption
from land_consumption.input import ComputeInput
from land_consumption.utils import (
    calculate_area,
    get_categories_gdf,
    custom_land_use_sort,
)

log = logging.getLogger(__name__)


class LandConsumption(BaseOperator[ComputeInput]):
    def __init__(self, ohsome_catalog: RestCatalog):
        super().__init__()
        self.ohsome_catalog = ohsome_catalog
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
            purpose=Path('resources/info/purpose.md'),
            methodology=Path('resources/info/methodology.md'),
            sources=Path('resources/info/sources.bib'),
            demo_input_parameters=ComputeInput(),
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
        log.info(
            f'Handling compute request: {params.model_dump()} in region {aoi_properties.model_dump()} in context: {resources}'
        )

        aoi_area = calculate_area(gpd.GeoDataFrame(geometry=[aoi], crs=4326)).iloc[0]['area']

        categories_gdf = get_categories_gdf(aoi, catalog=self.ohsome_catalog)

        log.info('Calculating area for each category')
        categories_gdf = calculate_area(categories_gdf)

        land_consumption_df = calculate_land_consumption(
            aoi_area=aoi_area,
            area_df=categories_gdf,
        )

        landobjects_consumption_table = LandConsumption.get_basic_table(land_consumption_df)

        landconsumer_consumption_table = LandConsumption.get_detailed_table(land_consumption_df)

        landconsumer_table_artifact = build_table_artifact(
            data=landconsumer_consumption_table,
            primary=False,
            resources=resources,
            title='Detailed Report',
            filename_suffix='consumer',
        )
        landobjects_table_artifact = build_table_artifact(
            data=landobjects_consumption_table,
            primary=True,
            resources=resources,
            title='Basic Report',
            filename_suffix='objects',
        )

        artifacts = [landconsumer_table_artifact, landobjects_table_artifact]
        log.debug(f'Returning {len(artifacts)} artifacts.')

        return artifacts

    @staticmethod
    def get_basic_table(land_consumption_df: pd.DataFrame) -> pd.DataFrame:
        log.debug('Creating basic table artifact for land consumption data.')

        land_consumption_df = (
            land_consumption_df.groupby('Land Use Object', as_index=False)
            .sum(numeric_only=True)
            .merge(
                land_consumption_df['Land Use Object'].drop_duplicates(),
                on='Land Use Object',
                how='left',
            )
        )
        land_consumption_df.loc[land_consumption_df['Land Use Object'] == 'Other', '% of Consumed Land Area'] = None

        total_land_area = land_consumption_df['Total Land Area [ha]'].sum()
        total_row = pd.DataFrame(
            {
                'Land Use Object': ['Total'],
                'Total Land Area [ha]': [total_land_area],
                '% of Consumed Land Area': [100.0],
                '% Land Area': [100.0],
            }
        )

        other_row = land_consumption_df[land_consumption_df['Land Use Object'] == 'Other']
        land_consumption_df = land_consumption_df[land_consumption_df['Land Use Object'] != 'Other']

        land_consumption_table = pd.concat([land_consumption_df, other_row], ignore_index=True)
        land_consumption_table = pd.concat([land_consumption_table, total_row], ignore_index=True)

        land_consumption_table.set_index('Land Use Object', inplace=True)

        return land_consumption_table

    def get_detailed_table(land_consumption_df: pd.DataFrame) -> pd.DataFrame:
        log.debug('Creating detailed table artifact for land consumption data.')

        total_land_area = land_consumption_df['Total Land Area [ha]'].sum()
        total_row = pd.DataFrame(
            {
                'Land Use Object': ['Total'],
                'Land Use Class': [''],
                'Total Land Area [ha]': [total_land_area],
                '% of Consumed Land Area': [100.0],
                '% Land Area': [100.0],
            }
        )

        other_row = land_consumption_df[land_consumption_df['Land Use Object'] == 'Other']
        land_consumption_df = land_consumption_df[land_consumption_df['Land Use Object'] != 'Other']
        subtotal_land_consumed = land_consumption_df.groupby('Land Use Object', as_index=False).sum(numeric_only=True)
        subtotal_land_consumed['Land Use Class'] = 'Subtotal'

        land_consumption_df = pd.concat([land_consumption_df, subtotal_land_consumed])

        land_consumption_df['sort_order'] = land_consumption_df.apply(custom_land_use_sort, axis=1)
        land_consumption_df = land_consumption_df.sort_values(['Land Use Object', 'sort_order', 'Land Use Class']).drop(
            columns=['sort_order']
        )

        land_consumption_table = pd.concat([land_consumption_df, other_row], ignore_index=True)
        land_consumption_table = pd.concat([land_consumption_table, total_row], ignore_index=True)
        land_consumption_table['Land Use Object'] = land_consumption_table['Land Use Object'].mask(
            land_consumption_table['Land Use Object'].duplicated(), ''
        )

        land_consumption_table.set_index('Land Use Object', inplace=True)

        return land_consumption_table
