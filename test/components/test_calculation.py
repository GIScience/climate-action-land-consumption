import pytest
from shapely import Polygon

from land_consumption.components.calculation import aggregate_by_categories, calculate_area
import geopandas as gpd
from land_consumption.components.landuse_category_mappings import LandObjectCategory, LandUseCategory
from approvaltests import verify


def test_aggregate_by_categories():
    area_df = gpd.GeoDataFrame(
        {
            'category': [
                LandObjectCategory.BUILDINGS,
                LandObjectCategory.BUILT_UP,
                LandObjectCategory.BUILT_UP,
                LandObjectCategory.OTHER,
            ],
            'landuse_category': [
                LandUseCategory.COMMERCIAL,
                LandUseCategory.AGRICULTURAL,
                LandUseCategory.NATURAL,
                LandUseCategory.OTHER,
            ],
            'area': [4000, 3000, 500, 500],
        }
    )

    result = aggregate_by_categories(area_df)

    verify(result.to_json(indent=2))


def test_calculate_area(multi_polygon):
    input = gpd.GeoDataFrame(
        geometry=[multi_polygon],
        crs=4326,
    )

    expected_output = pytest.approx(93176.13531065645)

    calculated_area = calculate_area(input)

    assert calculated_area['area'].sum() == expected_output


def test_calculate_area_multiple_geometries():
    polygon1 = Polygon([(0, 0), (2, 0), (2, 2), (0, 2), (0, 0)])
    polygon2 = Polygon([(3, 3), (5, 3), (5, 5), (3, 5), (3, 3)])

    input_gdf = gpd.GeoDataFrame(
        geometry=[polygon1, polygon2],
        crs=4326,
    )

    expected_output = pytest.approx(98350447453)

    calculated_area = calculate_area(input_gdf)

    assert calculated_area['area'].sum() == expected_output
