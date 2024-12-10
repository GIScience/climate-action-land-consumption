import pandas as pd
from pathlib import Path

from climatoology.base.artifact import (
    create_table_artifact,
    _Artifact,
)
from climatoology.base.computation import ComputationResources


def build_table_artifact(data: pd.DataFrame, resources: ComputationResources) -> _Artifact:
    data = data.round(2)
    return create_table_artifact(
        data=data,
        title='Land Consumption by Land Use Type',
        caption='The proportion of land consumed by different land uses, weighted by soil sealing factors.',
        description=Path('resources/info/description.md').read_text(),
        resources=resources,
        filename='table_landconsumption',
    )
