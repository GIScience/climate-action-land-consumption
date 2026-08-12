import uuid
from pathlib import Path

import geopandas as gpd
import pytest
import shapely
from climatoology.base.baseoperator import AoiProperties
from climatoology.base.computation import ComputationScope
from dotenv import load_dotenv
from ohsome_py2.client import OhsomeClient
from shapely.geometry.polygon import Polygon

from land_consumption.components.landuse_category_mappings import LandObjectCategory
from land_consumption.core.input import ComputeInput
from land_consumption.core.operator_worker import LandConsumption

load_dotenv()  # To load the `OHSOME_BASE_URL` environment variable, for recording new cassettes
pytest_plugins = ('ohsome_py2.test.fixtures',)


@pytest.fixture
def default_aoi() -> shapely.MultiPolygon:
    return shapely.MultiPolygon(
        polygons=[
            [
                [
                    [8.66, 49.425],
                    [8.66, 49.43],
                    [8.67, 49.43],
                    [8.67, 49.425],
                    [8.66, 49.425],
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
def default_operator(parametrized_ohsome_client):
    return LandConsumption(ohsome_client=parametrized_ohsome_client)


@pytest.fixture(params=[True, False])
def parametrized_ohsome_client(request):
    return OhsomeClient(user_agent='Land-Consumption Test', v2=request.param)


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


@pytest.fixture
def categories_gdf():
    """Fixture to create a sample GeoDataFrame for testing."""
    # Create sample geometries
    data = {
        'category': [
            LandObjectCategory.BUILDINGS.name,
            LandObjectCategory.PARKING_LOTS.name,
            LandObjectCategory.ROADS.name,
        ],
        'geometry': [
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),  # Building polygon
            Polygon([(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5), (0.5, 0.5)]),  # Parking lot polygon
            Polygon([(0, 1), (2, 1), (2, 2), (0, 2), (0, 1)]),  # Paved road polygon
        ],
    }
    gdf = gpd.GeoDataFrame(data, crs='EPSG:4326')
    return gdf


@pytest.fixture(scope='module')
def vcr_cassette_dir(request):
    file_name = Path(request.module.__file__).stem
    cassette_dir = str(Path(__file__).parent / 'resources' / 'vcr_cassettes' / file_name)
    return cassette_dir


@pytest.fixture(scope='module')
def vcr_config(vcr_config_ohsomepy2):
    return {
        **vcr_config_ohsomepy2,
        'filter_headers': ['authorization'],
        # Match on the default parts plus the request body
        'match_on': ['method', 'scheme', 'host', 'port', 'path', 'query', 'body'],
    }
