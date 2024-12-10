import ast
import uuid
from functools import partial
from unittest.mock import patch, MagicMock

import geopandas as gpd
import pytest
import rasterio
import responses
import shapely
from climatoology.base.computation import ComputationScope
from climatoology.base.baseoperator import AoiProperties
from climatoology.utility.api import LabelDescriptor, LabelResponse
from ohsome import OhsomeClient
from shapely.geometry.polygon import Polygon
from urllib3 import Retry

from land_consumption.input import ComputeInput
from land_consumption.operator_worker import LandConsumption
from land_consumption.utils import LandUseCategory


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


@pytest.fixture
def mock_get_osm_from_parquet():
    osm_from_parquet_gdf = gpd.read_file('resources/test/osm_from_parquet_response.geojson')
    osm_from_parquet_gdf['tags'] = osm_from_parquet_gdf['tags'].apply(lambda x: ast.literal_eval(x))

    with patch('land_consumption.utils.get_osm_data_from_parquet') as mock_gdf:
        mock_gdf.return_value = osm_from_parquet_gdf
        yield mock_gdf


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
def multi_polygon():
    return shapely.MultiPolygon(
        [
            [
                [
                    (8.692079588124045, 49.41054080364265),
                    (8.692079588124045, 49.4081998269551),
                    (8.697014933561888, 49.4081998269551),
                    (8.697014933561888, 49.41054080364265),
                    (8.692079588124045, 49.41054080364265),
                ]
            ]
        ]
    )


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


@pytest.fixture(scope='module')
def roads_df():
    return gpd.read_file('resources/test/roads_response.geojson')


@pytest.fixture(scope='module')
def mock_iceberg_scan(roads_df):
    df = roads_df.copy()
    mock_scan_result = MagicMock()

    # Convert tags to dict and geometry to WKT to match the expected output
    df['tags'] = df['tags'].apply(lambda x: dict(ast.literal_eval(x)))
    mock_scan_result.return_value = df

    return mock_scan_result


@pytest.fixture
def categories_gdf():
    """Fixture to create a sample GeoDataFrame for testing."""
    # Create sample geometries
    data = {
        'category': [
            LandUseCategory.BUILDINGS.name,
            LandUseCategory.PARKING_LOTS.name,
            LandUseCategory.PAVED_ROADS.name,
        ],
        'geometry': [
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),  # Building polygon
            Polygon([(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5), (0.5, 0.5)]),  # Parking lot polygon
            Polygon([(0, 1), (2, 1), (2, 2), (0, 2), (0, 1)]),  # Paved road polygon
        ],
    }
    gdf = gpd.GeoDataFrame(data, crs='EPSG:4326')
    return gdf
