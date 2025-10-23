import ast

import geopandas as gpd
import pytest
from climatoology.utility.exception import ClimatoologyUserError
from ohsome import OhsomeClient
from shapely.geometry.linestring import LineString
from shapely.geometry.polygon import Polygon

from land_consumption.utils import (
    LandObjectCategory,
    assign_road_width,
    calculate_area,
    check_path_count,
    clip_geometries,
    clip_to_aoi,
    generate_buffer,
    get_categories_gdf,
    get_land_object_filter,
    get_number_of_lanes,
    get_road_type,
    get_width_value,
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
        get_land_object_filter(LandObjectCategory.OTHER)


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


@pytest.mark.vcr
def test_get_categories_gdf_no_features(default_aoi):
    categories_gdf = get_categories_gdf(default_aoi, data_connection=OhsomeClient(user_agent='Land-Consumption Test'))

    assert not categories_gdf.empty
    assert 'category' in categories_gdf.columns


@pytest.mark.vcr
def test_get_categories_gdf_with_features(default_aoi):
    categories_gdf = get_categories_gdf(default_aoi, data_connection=OhsomeClient(user_agent='Land-Consumption Test'))

    assert not categories_gdf.empty
    assert 'category' in categories_gdf.columns
    assert categories_gdf.union_all().area == pytest.approx(default_aoi.area)


def test_check_path_count(default_aoi, responses_mock):
    with open('resources/test/ohsome_count_response.json', 'rb') as paths_count:
        responses_mock.post(
            'https://api.ohsome.org/v1/elements/count',
            body=paths_count.read(),
        )

    # test false situation
    with pytest.raises(ClimatoologyUserError):
        check_path_count(default_aoi, OhsomeClient(), 5000, row_filter='geometry:line')


def test_check_path_count_polygon(default_aoi):
    check_path_count(default_aoi, OhsomeClient(), 1, row_filter='geometry:polygon')


def test_clip_geometries_no_interior_intersection(categories_gdf):
    categories_gdf['category'] = categories_gdf['category'].map(lambda x: LandObjectCategory[x])
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


def test_clip_to_aoi():
    aoi_geom = Polygon(
        [
            (16.369602655, 48.21069154),
            (16.403734349, 48.21069154),
            (16.403734349, 48.229465035),
            (16.369602655, 48.229465035),
            (16.369602655, 48.21069154),
        ]
    )
    polygon_gdf = gpd.read_file('resources/test/test_clip_to_aoi_data.gpkg')
    geom_type = "'Polygon', 'MultiPolygon'"

    polygon_gdf = clip_to_aoi(polygon_gdf=polygon_gdf, aoi_geom=aoi_geom, geom_type=geom_type)

    assert all(polygon_gdf.is_valid)


# @pytest.mark.vcr
# def test_get_osm_data_from_parquet():
#     # VCR cassete can record the get-request but not the s3 pre-flight meaning that this test takes a long time and does a live call
#     settings = Settings()

#     ohsome_catalog = RestCatalog(
#     name='default',
#     **{
#         'uri': settings.ohsome_iceberg_uri,
#         's3.endpoint': settings.ohsome_minio_endpoint,
#         'py-io-impl': 'pyiceberg.io.pyarrow.PyArrowFileIO',
#         's3.access-key-id': settings.ohsome_minio_access_key_id,
#         's3.secret-access-key': settings.ohsome_minio_access_key,
#         's3.region': settings.ohsome_minio_region,
#     },
#     )

#     geom = shapely.from_wkt('POLYGON((8.66927799765881 49.41700150446267,8.67329058234509 49.41700150446267,8.67329058234509 49.41563347745208,8.66927799765881 49.41563347745208,8.66927799765881 49.41700150446267))')
#     row_filter = (
#         f"status = 'latest' "
#         f"and geometry_type IN ('Polygon') "
#         f'and (xmax >= {geom.bounds[0]} and xmin <= {geom.bounds[2]}) '
#         f'and (ymax >= {geom.bounds[1]} and ymin <= {geom.bounds[3]}) '
#     )
#     data = get_osm_data_from_parquet(    aoi_geom=geom,
#     row_filter=row_filter,
#     selected_fields= ('tags', 'geometry'),
#     catalog=ohsome_catalog,)

#     expected_data = gpd.GeoDataFrame(crs=4326)

#     geopandas.testing.assert_geodataframe_equal(left=expected_data,right=data)
