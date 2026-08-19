import pandas as pd
from climatoology.base.artifact import Artifact, ArtifactMetadata
from climatoology.base.artifact_creators import create_plotly_chart_artifact, create_table_artifact
from climatoology.base.computation import ComputationResources
from plotly.graph_objs import Figure

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


def build_table_artifact(data: pd.DataFrame, resources: ComputationResources, title: str) -> Artifact:
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
