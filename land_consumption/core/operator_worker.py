import logging
from typing import List

import shapely
from climatoology.base.baseoperator import AoiProperties, Artifact, BaseOperator, ComputationResources
from climatoology.base.plugin_info import PluginInfo
from ohsome_py2.client import OhsomeClient

from land_consumption.components.land_consumption import get_land_consumption_artifacts
from land_consumption.core.info import get_info
from land_consumption.core.input import ComputeInput

log = logging.getLogger(__name__)


class LandConsumption(BaseOperator[ComputeInput]):
    def __init__(self, ohsome_client: OhsomeClient):
        super().__init__()
        self.ohsome_client = ohsome_client

        log.debug('Initialised Land consumption Operator')

    def info(self) -> PluginInfo:
        return get_info()

    def compute(  # dead: disable
        self,
        resources: ComputationResources,
        aoi: shapely.MultiPolygon,
        aoi_properties: AoiProperties,
        params: ComputeInput,
    ) -> List[Artifact]:
        log.info(
            f'Handling compute request: {params.model_dump()} in region {aoi_properties.model_dump()} in context: {resources}'
        )
        artifacts = get_land_consumption_artifacts(aoi, ohsome_client=self.ohsome_client, resources=resources)

        return artifacts
