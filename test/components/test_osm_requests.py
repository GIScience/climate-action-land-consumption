import pytest
from climatoology.base.exception import ClimatoologyUserError
from ohsome_filter_to_sql.main import ohsome_filter_to_sql

from land_consumption.components.osm_requests import build_ohsome_filter, check_path_count


@pytest.mark.vcr
def test_check_path_count(default_aoi, default_ohsome_client_v1):
    with pytest.raises(ClimatoologyUserError):
        check_path_count(default_aoi, default_ohsome_client_v1, 10, row_filter='geometry:line')


def test_check_path_count_polygon(default_aoi, default_ohsome_client_v1):
    check_path_count(default_aoi, default_ohsome_client_v1, 1, row_filter='geometry:polygon')


def test_build_ohsome_filter():
    for geometry_type in ["'LineString', 'MultiLineString'", "'Polygon', 'MultiPolygon'"]:
        ohsome_filter = build_ohsome_filter(geometry_type)
        ohsome_filter_to_sql(ohsome_filter)
