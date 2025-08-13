import logging
from typing import List

import pandas as pd
import shapely
import plotly.express as px
from plotly.graph_objs import Figure
from climatoology.base.baseoperator import BaseOperator, _Artifact, AoiProperties, ComputationResources
from climatoology.base.info import _Info
from pyiceberg.catalog.rest import RestCatalog

from land_consumption.artifact import build_table_artifact, build_treemap_artifact
from land_consumption.calculation import calculate_land_consumption
from land_consumption.info import get_info
from land_consumption.input import ComputeInput
from land_consumption.utils import (
    calculate_area,
    get_categories_gdf,
    sort_land_consumption_table,
)

log = logging.getLogger(__name__)


class LandConsumption(BaseOperator[ComputeInput]):
    def __init__(self, ohsome_catalog: RestCatalog):
        super().__init__()
        self.ohsome_catalog = ohsome_catalog
        log.debug('Initialised Land consumption Operator')

    def info(self) -> _Info:
        return get_info()

    def compute(  # dead: disable
        self,
        resources: ComputationResources,
        aoi: shapely.MultiPolygon,
        aoi_properties: AoiProperties,
        params: ComputeInput,
    ) -> List[_Artifact]:
        log.info(
            f'Handling compute request: {params.model_dump()} in region {aoi_properties.model_dump()} in context: {resources}'
        )

        categories_gdf = get_categories_gdf(aoi, catalog=self.ohsome_catalog)

        log.info('Calculating area for each category')
        categories_gdf = calculate_area(categories_gdf)

        land_consumption_df = calculate_land_consumption(
            area_df=categories_gdf,
        )

        landobjects_consumption_table = self.get_basic_table(land_consumption_df)

        landconsumer_consumption_table = self.get_detailed_table(land_consumption_df)

        landconsumer_table_artifact = build_table_artifact(
            data=landconsumer_consumption_table,
            primary=False,
            resources=resources,
            title='detailed',
        )
        landobjects_table_artifact = build_table_artifact(
            data=landobjects_consumption_table,
            primary=False,
            resources=resources,
            title='basic',
        )

        treemap = self.create_treemap(land_consumption_df)

        treemap_artifact = build_treemap_artifact(
            figure=treemap,
            resources=resources,
            primary=True,
        )

        artifacts = [landconsumer_table_artifact, landobjects_table_artifact, treemap_artifact]

        log.debug(f'Returning {len(artifacts)} artifacts.')

        return artifacts

    @staticmethod
    def get_basic_table(land_consumption_df: pd.DataFrame) -> pd.DataFrame:
        log.debug('Creating basic table artifact for land consumption data.')

        land_consumption_df = (
            land_consumption_df.groupby('Land Use Object', as_index=False)
            .sum(numeric_only=True, min_count=1)
            .merge(
                land_consumption_df['Land Use Object'].drop_duplicates(),
                on='Land Use Object',
                how='left',
            )
        )

        total_land_area = land_consumption_df['Total Land Area [ha]'].sum()
        total_row = pd.DataFrame(
            {
                'Land Use Object': ['Total'],
                '% of Consumed Land Area': [100.0],
                '% of Settled Land Area': [100.0],
                'Total Land Area [ha]': [total_land_area],
                '% of Total Land Area': [100.0],
            }
        )

        land_consumption_table = pd.concat([land_consumption_df, total_row], ignore_index=True)
        land_consumption_table = sort_land_consumption_table(land_consumption_table)

        return land_consumption_table

    @staticmethod
    def get_detailed_table(land_consumption_df: pd.DataFrame) -> pd.DataFrame:
        log.debug('Creating detailed table artifact for land consumption data.')

        total_land_area = land_consumption_df['Total Land Area [ha]'].sum()
        total_row = pd.DataFrame(
            {
                'Land Use Object': ['Total'],
                'Land Use Class': [''],
                '% of Consumed Land Area': [100.0],
                '% of Settled Land Area': [100.0],
                'Total Land Area [ha]': [total_land_area],
                '% of Total Land Area': [100.0],
            }
        )

        subtotal_land_consumed = (
            land_consumption_df[
                ~land_consumption_df['Land Use Object'].isin(['Agricultural land', 'Natural land', 'Other'])
            ]
            .groupby('Land Use Object', as_index=False)
            .sum(numeric_only=True)
        )
        subtotal_land_consumed['Land Use Class'] = 'Subtotal'

        land_consumption_table = pd.concat([land_consumption_df, subtotal_land_consumed, total_row], ignore_index=True)

        land_consumption_table = sort_land_consumption_table(land_consumption_table, use_detailed_sort=True)

        land_consumption_table = land_consumption_table.reset_index()
        land_consumption_table['Land Use Object'] = land_consumption_table['Land Use Object'].astype(str)
        land_consumption_table['Land Use Object'] = land_consumption_table['Land Use Object'].mask(
            land_consumption_table['Land Use Object'].duplicated(), ''
        )
        land_consumption_table.set_index('Land Use Object', inplace=True)

        return land_consumption_table

    @staticmethod
    def create_treemap(landconsumer_consumption_table: pd.DataFrame) -> Figure:
        log.debug('Creating treemap for land consumption.')

        landconsumer_consumption_table = landconsumer_consumption_table.fillna(0)

        landconsumer_consumption_table['label'] = landconsumer_consumption_table.apply(
            lambda row: (
                f'{row["Land Use Class"]}<br><br>'
                f'% of Consumed Land Area: {row["% of Consumed Land Area"]}%<br>'
                f'% of Settled Land Area: {row["% of Settled Land Area"]}%<br>'
                f'Total Land Area [ha]: {row["Total Land Area [ha]"]} ha<br>'
                f'% of Total Land Area: {row["% of Total Land Area"]}%'
            ),
            axis=1,
        )

        treemap = px.treemap(
            landconsumer_consumption_table,
            path=[px.Constant('Land Use Overview'), 'Land Use Object', 'Land Use Class'],
            values='Total Land Area [ha]',
            color='Land Use Object',
            custom_data=['label', 'Land Use Object', 'Land Use Class'],
            color_discrete_map={
                '(?)': '#ffffe5',
                'Buildings': '#ec7014',
                'Agricultural land': '#fee391',
                'Roads': '#993404',
                'Parking lots': '#662506',
                'Built up land': '#fec44f',
                'Natural land': '#c7e9c0',
            },
        )

        treemap.update_traces(
            texttemplate='%{customdata[0]}',
            textinfo='text',
            hovertemplate=(
                'Land Use Object: %{customdata[1]}<br>'
                + 'Land Use Class: %{customdata[2]}<br>'
                + 'Total Land Area [ha]: %{value} ha<br>'
                + '<extra></extra>'
            ),
        )

        treemap.update_layout(
            title='Land Consumption by Land Use Object and Class',
            margin=dict(t=50, l=25, r=25, b=25),
            paper_bgcolor='white',
            plot_bgcolor='white',
        )

        # The code below is to remove plotly's autogenerated null values when it creates parent classes
        trace = treemap.data[0]

        for i in range(trace.customdata.shape[0]):
            for j in range(trace.customdata.shape[1]):
                if trace.customdata[i][j] == '(?)':
                    trace.customdata[i][j] = ''

        trace.labels = [label if label != '(?)' else '' for label in trace.labels]

        return treemap
