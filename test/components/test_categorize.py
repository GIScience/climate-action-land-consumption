import pytest
from ohsome import OhsomeClient

from land_consumption.components.categorize import get_land_object_filter, get_categories_gdf
from land_consumption.components.landuse_category_mappings import LandObjectCategory


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
