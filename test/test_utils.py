import ast
from unittest.mock import patch

import geopandas as gpd
import pytest
from shapely.geometry.linestring import LineString
from shapely.geometry.polygon import Polygon

from land_consumption.utils import (
    LandObjectCategory,
    calculate_area,
    assign_road_width,
    generate_buffer,
    get_land_object_filter,
    get_number_of_lanes,
    get_road_type,
    get_width_value,
    get_categories_gdf,
    clip_geometries,
)


def test_get_filter_functions():
    buildings_filter = get_land_object_filter(LandObjectCategory.BUILDINGS)
    assert buildings_filter({'building': 'yes'}) is True
    assert buildings_filter({'building': 'apartments'}) is True
    assert buildings_filter({'building': 'no'}) is False

    roads_filter = get_land_object_filter(LandObjectCategory.ROADS)
    assert roads_filter({'highway': 'primary'}) is True
    assert roads_filter({}) is False

    with pytest.raises(ValueError):
        get_land_object_filter(LandObjectCategory.UNKNOWN)


def test_get_width_value():
    assert get_width_value({'width': '5'}) == '5'
    assert get_width_value({}) is None


def test_get_road_type():
    assert get_road_type({'highway': 'primary'}) == 'primary'
    assert get_road_type({}) is None


def test_get_number_of_lanes():
    assert get_number_of_lanes({'lanes': 2}) == 2
    assert get_number_of_lanes({}) == 1


def test_assign_road_width():
    df = gpd.GeoDataFrame(
        {
            'tags': [{'highway': 'primary', 'width': '10'}, {'highway': 'secondary'}],
            'geometry': [LineString([(0, 0), (1, 1)]), LineString([(1, 1), (2, 2)])],
        }
    )
    result = assign_road_width(df)
    assert 'width' in result.columns
    assert result.loc[1, 'width'] == 3  # Default width for missing values


def test_generate_buffer():
    df = gpd.GeoDataFrame(
        {'geometry': [LineString([(0, 0), (1, 1)])], 'width': [10]}, geometry='geometry', crs='EPSG:4326'
    )
    buffered = generate_buffer(df)
    assert buffered.geometry.iloc[0].area > 0


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


def test_generate_buffer_from_gdf(roads_df):
    roads_df['tags'] = roads_df['tags'].apply(lambda x: dict(ast.literal_eval(x)))

    roads_with_width = assign_road_width(roads_df)

    buffered_roads = generate_buffer(roads_with_width)

    assert 'geometry' in buffered_roads.columns

    assert buffered_roads['geometry'].geom_type.eq('MultiPolygon').all()

    assert ~buffered_roads['geometry'].is_empty.all() & buffered_roads['geometry'].notna().all()


@patch('land_consumption.utils.get_osm_data_from_parquet')
def test_get_categories_gdf_no_features(mock_get_osm_data, default_ohsome_catalog):
    mock_get_osm_data.return_value = gpd.GeoDataFrame(
        {
            'tags': [{'building': 'no'}, {'no_highway': 'primary'}, {'landuse': 'commercial'}],
            'geometry': [
                Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                LineString([(0, 0), (1, 1)]),
                Polygon([(0, 0), (2, 0), (2, 2), (0, 2)]),
            ],
        }
    )

    aoi_geom = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    categories_gdf = get_categories_gdf(aoi_geom, catalog=default_ohsome_catalog)

    assert not categories_gdf.empty
    assert 'category' in categories_gdf.columns


@patch('land_consumption.utils.get_osm_data_from_parquet')
def test_get_categories_gdf_with_features(mock_get_osm_data, default_ohsome_catalog):
    mock_get_osm_data.return_value = gpd.GeoDataFrame(
        {
            'tags': [
                {'building': 'yes'},
                {'highway': 'primary'},
                {'amenity': 'parking', 'parking': 'surface'},
                {'landuse': 'residential'},
            ]
        },
        geometry=[
            Polygon([(0, 0), (0.25, 0), (0.25, 0.25), (0, 0.25)]),
            LineString([(0, 0), (1, 1)]),
            Polygon([(0, 0), (0.5, 0), (0.5, 0.5), (0, 0.5)]),
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
        ],
    )

    aoi_geom = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    categories_gdf = get_categories_gdf(aoi_geom, catalog=default_ohsome_catalog)

    assert not categories_gdf.empty
    assert 'category' in categories_gdf.columns
    assert categories_gdf.unary_union.area == pytest.approx(aoi_geom.area)


def test_clip_geometries_no_interior_intersection(categories_gdf):
    result = clip_geometries(categories_gdf)

    assert result.crs.to_string() == 'EPSG:4326'

    # Check that interiors of geometries do not intersect
    for i, geom1 in enumerate(result['geometry']):
        for j, geom2 in enumerate(result['geometry']):
            if i != j:
                # DE-9IM relationship
                assert geom1.relate(geom2) in ('FF*FF****', 'F0FFFF212', 'FF2F11212')

    assert set(result['category']) == set(categories_gdf['category'])
    assert len(result) == len(categories_gdf)


# def test_geometries_all_intersecting(categories_gdf):
