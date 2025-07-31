import pandas as pd
from plotly.graph_objs import Figure

from climatoology.base.artifact import (
    create_table_artifact,
    create_plotly_chart_artifact,
    _Artifact,
)
from climatoology.base.computation import ComputationResources


def build_table_artifact(data: pd.DataFrame, resources: ComputationResources, primary: bool, title: str) -> _Artifact:
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
        caption = 'The proportion of land consumed by different land use objects.'
    else:
        caption = 'The proportion of land consumed by different land use objects and classes.'

    return create_table_artifact(
        data=data,
        title=f'{title.title()} Report',
        caption=caption,
        description=description,
        resources=resources,
        primary=primary,
        filename=filename,
    )


def build_treemap_artifact(figure: Figure, resources: ComputationResources, primary: bool) -> _Artifact:
    return create_plotly_chart_artifact(
        figure=figure,
        title='Land Consumption Treemap',
        caption=(
            'Click on the boxes to explore the Land Consumption treemap. To return to the top level, '
            'click “Land Use Overview.”'
        ),
        description=(
            'The Land Consumption Treemap hierarchically visualizes the proportion of land consumed in a '
            'given area by land use object (e.g. buildings) and by land use class (e.g. residential).'
        ),
        resources=resources,
        filename='land_consumption_treemap',
        primary=primary,
    )
