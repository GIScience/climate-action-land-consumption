import uuid
from pathlib import Path
from typing import List
from unittest.mock import patch

import pytest
import rasterio
from climatoology.base.artifact import ArtifactModality
from climatoology.base.computation import ComputationScope
from climatoology.base.operator import Concern, Info, PluginAuthor, _Artifact
from climatoology.utility.api import LabelDescriptor, LabelResponse

from land_consumption.input import ComputeInput
from land_consumption.operator_worker import LandConsumption


@pytest.fixture
def expected_compute_input() -> ComputeInput:
    # noinspection PyTypeChecker
    return ComputeInput(
        aoi={
            'type': 'Feature',
            'properties': {'name': 'Heidelberg', 'id': 'Q12345'},
            'geometry': {
                'type': 'MultiPolygon',
                'coordinates': [
                    [
                        [
                            [12.3, 48.22],
                            [12.3, 48.34],
                            [12.48, 48.34],
                            [12.48, 48.22],
                            [12.3, 48.22],
                        ]
                    ]
                ],
            },
        },
    )


@pytest.fixture
def expected_info_output() -> Info:
    # noinspection PyTypeChecker
    return Info(
        name='Land Consumption',
        icon=Path('resources/info/icon.jpeg'),
        authors=[
            PluginAuthor(
                name='Charles Hatfield',
                affiliation='HeiGIT gGmbH',
                website='https://heigit.org/heigit-team/',
            ),
            PluginAuthor(
                name='Jonas Kemmer',
                affiliation='HeiGIT gGmbH',
                website='https://heigit.org/heigit-team/',
            ),
        ],
        version='dummy',
        concerns=[Concern.CLIMATE_ACTION__GHG_EMISSION],
        purpose=Path('resources/info/purpose.md').read_text(),
        methodology=Path('resources/info/methodology.md').read_text(),
        sources=Path('resources/info/sources.bib'),
    )


@pytest.fixture
def expected_compute_output(compute_resources) -> List[_Artifact]:
    table_artifact = _Artifact(
        name='Land Consumption by Land Use Type',
        modality=ArtifactModality.TABLE,
        file_path=Path(compute_resources.computation_dir / 'table_landconsumption.csv'),
        summary='The proportion of land consumed by different land uses, weighted by land consumption factor.',
        description='A table with proportion of land consumed in a given area of interest. Consuming land in this '
        'context refers to the transformation of natural landscapes to artificial or seminatural '
        'landscapes. The resultant table therefore calculates how much natural land is left in an area '
        'versus how much has been consumed or transformed into artifical or seminatural land.',
    )
    return [
        table_artifact,
    ]


# The following fixtures can be ignored on plugin setup
@pytest.fixture
def compute_resources():
    with ComputationScope(uuid.uuid4()) as resources:
        yield resources


@pytest.fixture
def operator(lulc_utility):
    return LandConsumption(lulc_utility)


@pytest.fixture
def lulc_utility():
    with patch('climatoology.utility.api.LulcUtility') as lulc_utility:
        lulc_utility.compute_raster.return_value.__enter__.return_value = rasterio.open(
            'resources/test/segmentation.tiff'
        )
        lulc_utility.get_class_legend.return_value = LabelResponse(
            osm={
                'unknown': LabelDescriptor(
                    name='unknown',
                    osm_filter=None,
                    color=(0, 0, 0),
                    description='Class Unknown',
                    raster_value=0,
                )
            },
            corine={
                'unknown': LabelDescriptor(
                    name='unknown',
                    osm_filter=None,
                    color=(0, 0, 0),
                    description='Class Unknown',
                    raster_value=0,
                )
            },
        )
    yield lulc_utility
