import pytest
from ohsome import OhsomeClient

from land_consumption.components.categorize import get_land_object_filter, get_categories_gdf, get_land_use_filter
from land_consumption.components.landuse_category_mappings import LandObjectCategory, LandUseCategory


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


def test_get_landuse_filter():
    assert get_land_use_filter({'landuse': 'retail'}) == LandUseCategory.COMMERCIAL
    assert get_land_use_filter({'leisure': 'nature_reserve'}) == LandUseCategory.NATURAL
    assert get_land_use_filter({'amenity': 'university'}) == LandUseCategory.INSTITUTIONAL
    assert get_land_use_filter({'amenity': 'bus_station'}) == LandUseCategory.INFRASTRUCTURE
    assert get_land_use_filter({'natural': 'forest'}) == LandUseCategory.NATURAL
    assert get_land_use_filter({'natural': 'peninsula'}) == LandUseCategory.UNKNOWN
    assert get_land_use_filter({}) == LandUseCategory.UNKNOWN
    assert get_land_use_filter({'amenity': 'supermarket'}) == LandUseCategory.OTHER
