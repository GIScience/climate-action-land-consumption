import logging
from enum import StrEnum

import geopandas as gpd
import pandas as pd
from climatoology.base.artifact import Artifact, ArtifactMetadata, Legend
from climatoology.base.artifact_creators import (
    create_plotly_chart_artifact,
    create_table_artifact,
    create_vector_artifact,
)
from climatoology.base.computation import ComputationResources
from plotly.graph_objs import Figure
from pydantic_extra_types import Color

from land_consumption.components.land_use_category_mappings import LandObjectCategory, LandUseCategory

log = logging.getLogger(__name__)

LAND_CONSUMPTION_DEFINITIONS = (
    '\n\n**Settled Land:**\n'
    'Includes all land that is human-altered or developed such as agriculture, buildings, and paved areas, '
    'but excludes natural land.\n\n'
    '**Consumed Land:**\n'
    'Buildings, roads, parking areas, and other built infrastructure. It does not include agricultural, '
    'natural, or semi-natural areas.\n\n'
    '**Built-up Land:**\n'
    'Includes all land that is within an area tagged in OSM as commercial, residential, industrial, '
    'infrastructure, institutional, or other human-altered land uses but not directly assigned to a tag '
    'itself. In essence, built-up land refers to the interstitial spaces between other tags in consumed '
    'or settled areas. For example, this includes the land surrounding a building in a commercial area '
    'that is not tagged as a sidewalk or another feature.'
)


class Tags(StrEnum):
    MAP = 'map'
    TABLE = 'table'


def build_table_artifact(data: pd.DataFrame, resources: ComputationResources, title: str) -> Artifact:
    log.debug(f'Saving {title} table artifact')
    data = data.round(2)
    filename = f'table_landconsumption_{title}'
    if title == 'basic':
        description = ('How much land has been consumed by each land use object.') + LAND_CONSUMPTION_DEFINITIONS
    else:
        description = (
            'How much land has been consumed by each land use object and each land use class.'
        ) + LAND_CONSUMPTION_DEFINITIONS
    if title == 'basic':
        caption = 'The percentage of land consumed by different land use objects. Results depend on OSM data quality. If you can, contribute to OSM.'
    else:
        caption = 'The percentage of land consumed by different land use objects and classes. Results depend on OSM data quality. If you can, contribute to OSM.'

    table_artifact_metadata = ArtifactMetadata(
        name=f'{title.title()} Report',
        summary=caption,
        description=description,
        filename=filename,
        tags={Tags.TABLE},
    )
    return create_table_artifact(
        data=data,
        metadata=table_artifact_metadata,
        resources=resources,
    )


def build_treemap_artifact(
    figure: Figure,
    resources: ComputationResources,
) -> Artifact:
    log.debug('Saving Treemap Artifact')
    plotly_chart_artifact_metadata = ArtifactMetadata(
        name='Land Consumption Treemap',
        summary='Click on the boxes to explore the Land Consumption treemap. To return to the top level, '
        'click “Land Use Overview.” Results depend on OSM data quality. If you can, contribute to OSM.',
        description=(
            'Treemap visualization of the percentage of land consumed in an AOI '
            'by land use object (e.g. buildings) and by land use class (e.g. residential).'
        )
        + LAND_CONSUMPTION_DEFINITIONS,
        filename='land_consumption_treemap',
    )
    return create_plotly_chart_artifact(
        figure=figure,
        metadata=plotly_chart_artifact_metadata,
        resources=resources,
    )


def build_map_artifacts(categories_gdf: gpd.GeoDataFrame, resources: ComputationResources) -> list[Artifact]:
    log.debug('Saving map artifacts')
    categories_gdf['land_object'] = categories_gdf['land_object_category'].apply(lambda category: category.value)
    categories_gdf['land_use'] = categories_gdf['land_use_category'].apply(lambda category: category.value)

    land_object_color_map = {
        LandObjectCategory.BUILDINGS: '#ec7014',
        LandObjectCategory.PARKING_LOTS: '#662506',
        LandObjectCategory.ROADS: '#993404',
        LandObjectCategory.BUILT_UP: '#fec44f',
        LandObjectCategory.OTHER: '#882255',
        LandObjectCategory.UNKNOWN: '#bfbfbf',
    }

    land_use_color_map = {
        LandUseCategory.INDUSTRIAL: '#813399',  # lightness: 40
        LandUseCategory.COMMERCIAL: '#b466cc',  # lightness: 60
        LandUseCategory.INSTITUTIONAL: '#dab3e6',  # lightness: 80
        LandUseCategory.RESIDENTIAL: '#bf5340',  # lightness: 50
        LandUseCategory.INFRASTRUCTURE: '#4d4d4d',  # lightness 30
        LandUseCategory.AGRICULTURAL: '#d9d68c',  # lightness 70
        LandUseCategory.NATURAL: '#6ac653',  # lightness 55
        LandUseCategory.WATER: '#79b3d2',  # lightness 65
        LandUseCategory.OTHER: '#7d6436',  # lightness: 35
        LandUseCategory.UNKNOWN: '#bfbfbf',  # lightness 75
    }

    categories_gdf['land_object_color'] = (
        categories_gdf['land_object_category'].map(land_object_color_map).apply(lambda x: Color(x))
    )
    categories_gdf['land_use_color'] = (
        categories_gdf['land_use_category'].map(land_use_color_map).apply(lambda x: Color(x))
    )

    categories_gdf = categories_gdf.explode(ignore_index=True)

    land_object_gdf = categories_gdf.loc[
        ~categories_gdf['land_object_category'].isin(
            [LandObjectCategory.BUILT_UP, LandObjectCategory.OTHER, LandObjectCategory.UNKNOWN]
        )
    ]

    land_object_metadata = ArtifactMetadata(
        name='Land Object Map',
        summary='Map of land objects',
        description='This map shows land objects. For better visualisation, it focuses only on roads, buildings, and parking lots. For a description of the land object categories, see the methodology.',
        primary=False,
        tags={Tags.MAP},
        filename='land_object_map',
    )

    land_object_legend = Legend(
        title='Land Objects',
        legend_data={
            k.value: Color(v)
            for k, v in land_object_color_map.items()
            if k not in [LandObjectCategory.BUILT_UP, LandObjectCategory.OTHER, LandObjectCategory.UNKNOWN]
        },
    )

    land_object_artifact = create_vector_artifact(
        data=land_object_gdf[['land_object', 'geometry', 'land_object_color']],
        label='land_object',
        color='land_object_color',
        resources=resources,
        metadata=land_object_metadata,
        legend=land_object_legend,
    )

    land_use_metadata = ArtifactMetadata(
        name='Land Use Map',
        summary='Map of land use',
        description='This map shows all land uses. For a description of the land use categories, see the methodology.',
        primary=False,
        tags={Tags.MAP},
        filename='land_use_map',
    )

    land_use_legend = Legend(title='Land Uses', legend_data={k.value: Color(v) for k, v in land_use_color_map.items()})

    land_use_artifact = create_vector_artifact(
        data=categories_gdf[['land_use', 'geometry', 'land_use_color']],
        label='land_use',
        color='land_use_color',
        resources=resources,
        metadata=land_use_metadata,
        legend=land_use_legend,
    )

    return [land_object_artifact, land_use_artifact]
