import ast

import geopandas as gpd
from shapely import LineString

from land_consumption.components.process_geometries import (
    get_width_value,
    get_road_type,
    get_number_of_lanes,
    assign_road_width,
    generate_buffer,
)


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


def test_generate_buffer_from_gdf(roads_df):
    roads_df['tags'] = roads_df['tags'].apply(lambda x: dict(ast.literal_eval(x)))

    roads_with_width = assign_road_width(roads_df)

    buffered_roads = generate_buffer(roads_with_width)

    assert 'geometry' in buffered_roads.columns

    assert buffered_roads['geometry'].geom_type.eq('MultiPolygon').all()

    assert ~buffered_roads['geometry'].is_empty.all() & buffered_roads['geometry'].notna().all()
