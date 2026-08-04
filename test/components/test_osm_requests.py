import pytest
from climatoology.base.exception import ClimatoologyUserError
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
