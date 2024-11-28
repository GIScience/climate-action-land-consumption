import uuid
from functools import partial
from unittest.mock import patch

import geopandas as gpd
import pytest
import rasterio
import responses
import shapely
from climatoology.base.computation import ComputationScope
from climatoology.base.baseoperator import AoiProperties
from climatoology.utility.api import LabelDescriptor, LabelResponse
from ohsome import OhsomeClient
from urllib3 import Retry

from land_consumption.input import ComputeInput
from land_consumption.operator_worker import LandConsumption


@pytest.fixture
def default_aoi() -> shapely.MultiPolygon:
    return shapely.MultiPolygon(
        polygons=[
            [
                [
                    [12.3, 48.22],
                    [12.3, 48.34],
                    [12.48, 48.34],
                    [12.48, 48.22],
                    [12.3, 48.22],
                ]
            ]
        ]
    )


@pytest.fixture
def default_aoi_properties() -> AoiProperties:
    return AoiProperties(name='Heidelberg', id='heidelberg')


@pytest.fixture
def expected_compute_input() -> ComputeInput:
    # noinspection PyTypeChecker
    return ComputeInput()


# The following fixtures can be ignored on plugin setup
@pytest.fixture
def compute_resources():
    with ComputationScope(uuid.uuid4()) as resources:
        yield resources


@pytest.fixture
def responses_mock():
    with responses.RequestsMock() as rsps:
        yield rsps


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


@pytest.fixture(scope='module')
def bpolys():
    """Small bounding boxes."""
    bpolys = gpd.GeoSeries(
        data=[
            # Heidelberg (large box but not full city area)
            shapely.box(8.21920, 49.36622, 8.71928, 49.44017),
        ],
        crs='EPSG:4326',
    )
    return bpolys


@pytest.fixture(scope='module')
def request_ohsome(bpolys):
    return partial(
        OhsomeClient(
            user_agent='HeiGIT Climate Action Land Consumption Tester', retry=Retry(total=1)
        ).elements.geometry.post,
        bpolys=bpolys,
        properties='tags',
        time='2024-01-01',
        timeout=120,
    )
