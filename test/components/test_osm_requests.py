import pytest
from climatoology.utility.exception import ClimatoologyUserError
from ohsome import OhsomeClient
from ohsome_filter_to_sql.main import ohsome_filter_to_sql

from land_consumption.components.osm_requests import check_path_count, build_ohsome_filter


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


def test_build_ohsome_filter():
    for geometry_type in ["'LineString', 'MultiLineString'", "'Polygon', 'MultiPolygon'"]:
        ohsome_filter = build_ohsome_filter(geometry_type)
        ohsome_filter_to_sql(ohsome_filter)


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
