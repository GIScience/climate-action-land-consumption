import pandas as pd
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
        caption='The proportion of land consumed by different land uses, weighted by land consumption factor.',
        description='A table with proportion of land consumed in a given area of interest. Consuming land in this '
        'context refers to the transformation of natural landscapes to artificial or seminatural landscapes. The resultant '
        'table therefore calculates how much natural land is left in an area versus how much has been consumed or '
        'transformed into artifical or seminatural land.',
        resources=resources,
        filename='table_landconsumption',
    )
