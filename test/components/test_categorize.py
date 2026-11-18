import geopandas as gpd
import pytest

from land_consumption.components.categorize import (
    clean_overlapping_features,
    get_categories_gdf,
    get_land_object_filter,
    get_land_use_filter,
)
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
def test_get_categories_gdf_with_features(default_aoi, ohsome_client):
    categories_gdf = get_categories_gdf(default_aoi, ohsome_client=ohsome_client)

    assert not categories_gdf.empty
    assert 'land_object_category' in categories_gdf.columns
    assert categories_gdf.union_all().area == pytest.approx(default_aoi.area)


def test_get_landuse_filter():
    assert get_land_use_filter({'landuse': 'retail'}) == LandUseCategory.COMMERCIAL
    assert get_land_use_filter({'leisure': 'nature_reserve'}) == LandUseCategory.NATURAL
    assert get_land_use_filter({'amenity': 'university'}) == LandUseCategory.INSTITUTIONAL
    assert get_land_use_filter({'amenity': 'bus_station'}) == LandUseCategory.INFRASTRUCTURE
    assert get_land_use_filter({'natural': 'forest'}) == LandUseCategory.NATURAL
    assert get_land_use_filter({'natural': 'peninsula'}) == LandUseCategory.UNKNOWN
    assert get_land_use_filter({'natural': 'water'}) == LandUseCategory.OTHER
    assert get_land_use_filter({'natural': 'bay'}) == LandUseCategory.WATER
    assert get_land_use_filter({'natural': 'water', 'water': 'lake'}) == LandUseCategory.WATER
    assert get_land_use_filter({'natural': 'water', 'water': 'harbour'}) == LandUseCategory.INFRASTRUCTURE
    assert get_land_use_filter({}) == LandUseCategory.UNKNOWN
    assert get_land_use_filter({'amenity': 'supermarket'}) == LandUseCategory.OTHER


def test_clean_overlapping_features(categories_gdf, landuses_gdf):
    aoi_geom = landuses_gdf.union_all().buffer(2)
    result = clean_overlapping_features(categories_gdf.copy(), landuses_gdf.copy(), aoi_geom)

    actual_area = gpd.GeoSeries(result.geometry, crs=4326).area.sum()

    assert actual_area == pytest.approx(aoi_geom.area)


def test_clean_overlapping_features_no_overlap(categories_gdf, landuses_gdf):
    aoi_geom = landuses_gdf.union_all()
    result = clean_overlapping_features(categories_gdf.copy(), landuses_gdf.copy(), aoi_geom)

    assert result.area.sum() == result.union_all().area
