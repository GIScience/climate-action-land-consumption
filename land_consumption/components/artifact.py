import pandas as pd
from plotly.graph_objs import Figure
from climatoology.base.artifact import Artifact, ArtifactMetadata
from climatoology.base.artifact_creators import (
    create_table_artifact,
    create_plotly_chart_artifact,
)
from climatoology.base.computation import ComputationResources


def build_table_artifact(data: pd.DataFrame, resources: ComputationResources, title: str) -> Artifact:
    data = data.round(2)
    filename = f'table_landconsumption_{title}'
    if title == 'basic':
        description = (
            'The Basic Land Consumption Table reports how much land has been consumed by each land use '
            'object. E.g., how much land is consumed by buildings in Heidelberg.'
        )
    else:
        description = (
            'The Detailed Land Consumption Table reports how much land has been consumed by each land use '
            'object as well as by each land use class. E.g., how much land is consumed by buildings in '
            'commercial areas in Heidelberg.'
        )
    if title == 'basic':
        caption = 'The proportion of land consumed by different land use objects. Results depend on OSM data quality. If you can, contribute to OSM.'
    else:
        caption = 'The proportion of land consumed by different land use objects and classes. Results depend on OSM data quality. If you can, contribute to OSM.'

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
            'The Land Consumption Treemap hierarchically visualizes the proportion of land consumed in a '
            'given area by land use object (e.g. buildings) and by land use class (e.g. residential).'
        ),
        filename='land_consumption_treemap',
    )
    return create_plotly_chart_artifact(
        figure=figure,
        metadata=plotly_chart_artifact_metadata,
        resources=resources,
    )
