import geopandas as gpd
import pytest
import shapely
from ohsome import OhsomeClient

from land_consumption.utils import (
    LandUseCategory,
    get_ohsome_filter,
    calculate_area,
    fetch_osm_area,
)


def test_fetch_osm_area(responses_mock):
    with open('resources/test/ohsome_area_response.geojson', 'rb') as vector:
        responses_mock.post(
            'https://api.ohsome.org/v1/elements/area',
            body=vector.read(),
        )

    aoi_input = shapely.MultiPolygon([[[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]]])

    expected_osm_area = 1.0

    computed_osm_area = fetch_osm_area(aoi=aoi_input, osm_filter='dummy=yes', ohsome=OhsomeClient())

    assert computed_osm_area == expected_osm_area


def test_calculate_area():
    input = gpd.GeoSeries(
        data=[
            shapely.MultiPolygon(
                [[[(12.3, 48.22), (12.3, 48.2205), (12.3005, 48.2205), (12.3005, 48.22), (12.3, 48.22)]]]
            )
        ],
        crs=4326,
    )

    expected_output = pytest.approx(2066.027483292396)

    calculated_area = calculate_area(gdf_input=input)

    assert calculated_area == expected_output


def test_calculate_area_empty_input():
    input = gpd.GeoSeries(data=[], crs=4326)

    expected_output = 0.0

    calculated_area = calculate_area(gdf_input=input)

    assert calculated_area == expected_output


def test_get_ohsome_filter_building():
    expected_filter = 'geometry:polygon and building=* and not building=no'
    computed_filter = get_ohsome_filter(LandUseCategory.BUILDINGS)
    assert computed_filter == expected_filter


def test_get_ohsome_filter_parkinglot():
    expected_filter = 'geometry:polygon and amenity=parking and parking=surface'
    computed_filter = get_ohsome_filter(LandUseCategory.PARKING_LOTS)
    assert computed_filter == expected_filter


def test_get_ohsome_filter_unknown():
    with pytest.raises(ValueError, match='LandUseCategory.UNKNOWN'):
        get_ohsome_filter(LandUseCategory.UNKNOWN)
