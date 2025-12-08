import logging
from typing import List

import shapely
from _duckdb import DuckDBPyConnection
from climatoology.base.baseoperator import AoiProperties, BaseOperator, ComputationResources, _Artifact
from climatoology.base.info import _Info
from ohsome import OhsomeClient
from pyiceberg.catalog.rest import RestCatalog

from land_consumption.components.land_consumption import get_land_consumption_artifacts
from land_consumption.core.info import get_info
from land_consumption.core.input import ComputeInput

log = logging.getLogger(__name__)


class LandConsumption(BaseOperator[ComputeInput]):
    def __init__(self, data_connection: RestCatalog | tuple[RestCatalog, DuckDBPyConnection] | OhsomeClient):
        super().__init__()
        self.data_connection = data_connection
        if isinstance(data_connection, RestCatalog) or isinstance(data_connection, tuple):
            log.warning(f'Data connection {data_connection} is not tested')

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
        artifacts = get_land_consumption_artifacts(aoi, data_connection=self.data_connection, resources=resources)

        return artifacts
