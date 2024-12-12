import ast
import uuid
from unittest.mock import patch, MagicMock

import geopandas as gpd
import pytest
import shapely
from climatoology.base.baseoperator import AoiProperties
from climatoology.base.computation import ComputationScope
from pyiceberg.catalog.rest import RestCatalog
from requests_mock import Mocker
from shapely.geometry.polygon import Polygon

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
    return ComputeInput()


@pytest.fixture
def compute_resources():
    with ComputationScope(uuid.uuid4()) as resources:
        yield resources


@pytest.fixture
def operator(default_ohsome_catalog):
    return LandConsumption(ohsome_catalog=default_ohsome_catalog)


@pytest.fixture
def default_ohsome_catalog() -> RestCatalog:
    test_uri = 'https://test-uri'
    with Mocker() as m:
        m.get(
            f'{test_uri}/v1/config',
            json={'defaults': {}, 'overrides': {}},
            status_code=200,
        )
        catalog = RestCatalog(
            name='default',
            **{
                'uri': test_uri,
                's3.endpoint': '/',
                'py-io-impl': 'pyiceberg.io.pyarrow.PyArrowFileIO',
                's3.access-key-id': 'test-key-id',
                's3.secret-access-key': 'test-access-key',
                's3.region': 'eu-west-1',
            },
        )
        yield catalog


@pytest.fixture
def mock_get_osm_from_parquet():
    osm_from_parquet_gdf = gpd.read_file('resources/test/osm_from_parquet_response.geojson')
    osm_from_parquet_gdf['tags'] = osm_from_parquet_gdf['tags'].apply(lambda x: ast.literal_eval(x))

    with patch('land_consumption.utils.get_osm_data_from_parquet') as mock_gdf:
        mock_gdf.return_value = osm_from_parquet_gdf
        yield mock_gdf


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
