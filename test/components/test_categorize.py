import geopandas as gpd
import pytest
import shapely
from geopandas.testing import assert_geodataframe_equal

from land_consumption.components.categorize import (
    clean_land_uses,
    clean_overlapping_features,
    get_categories_gdf,
    get_land_object_filter,
    get_land_use_filter,
)
from land_consumption.components.land_use_category_mappings import LandObjectCategory, LandUseCategory


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


def test_get_land_use_filter():
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


def test_clean_overlapping_features(categories_gdf, land_uses_gdf):
    """Test that the entire aoi is covered"""
    aoi_geom = land_uses_gdf.union_all().buffer(2)
    result = clean_overlapping_features(categories_gdf.copy(), land_uses_gdf.copy(), aoi_geom)

    actual_area = gpd.GeoSeries(result.geometry, crs=4326).area.sum()

    assert actual_area == pytest.approx(aoi_geom.area)


def test_clean_overlapping_features_no_overlap(categories_gdf, land_uses_gdf):
    """Test that there's no overlapping features"""
    aoi_geom = land_uses_gdf.union_all()
    result = clean_overlapping_features(categories_gdf.copy(), land_uses_gdf.copy(), aoi_geom)

    assert result.area.sum() == result.union_all().area


@pytest.fixture
def simple_aoi():
    return shapely.box(0, 0, 1, 1)


@pytest.mark.parametrize(
    ['land_object_category', 'land_use_category'],
    argvalues=[
        (LandObjectCategory.BUILDINGS, LandUseCategory.UNKNOWN),
        (LandObjectCategory.ROADS, LandUseCategory.UNKNOWN),
        (LandObjectCategory.PARKING_LOTS, LandUseCategory.UNKNOWN),
    ],
)
def test_clean_overlapping_features_combinations(land_use_category, land_object_category, simple_aoi):
    land_object = gpd.GeoDataFrame(
        data={'land_object_category': [land_object_category]},
        geometry=[shapely.box(0, 0, 0.5, 0.5)],
    )

    land_use = gpd.GeoDataFrame(data={'land_use_category': [land_use_category]}, geometry=[simple_aoi])

    received = clean_overlapping_features(categories_gdf=land_object, land_uses_gdf=land_use, aoi=simple_aoi)

    assert received.loc[0, 'land_object_category'] == land_object_category


@pytest.mark.parametrize(
    'land_object_category',
    argvalues=[LandObjectCategory.ROADS, LandObjectCategory.PARKING_LOTS, LandObjectCategory.BUILDINGS],
)
def test_clean_overlapping_features_unknown_land_use(land_object_category, simple_aoi):
    object_geometry = shapely.box(2, 2, 2.5, 2.5)
    land_use_geometry = simple_aoi

    land_object = gpd.GeoDataFrame(
        data={'land_object_category': [land_object_category]},
        geometry=[object_geometry],
    )

    land_use = gpd.GeoDataFrame(data={'land_use_category': [LandUseCategory.RESIDENTIAL]}, geometry=[land_use_geometry])

    expected = gpd.GeoDataFrame(
        data={
            'land_object_category': [land_object_category, LandObjectCategory.BUILT_UP, LandObjectCategory.BUILT_UP],
            'land_use_category': [LandUseCategory.UNKNOWN, LandUseCategory.RESIDENTIAL, LandUseCategory.UNKNOWN],
        },
        geometry=[
            object_geometry,
            land_use_geometry,
            shapely.from_wkt('POLYGON ((2.5 0, 1 0, 1 1, 0 1, 0 2.5, 2 2.5, 2 2, 2.5 2, 2.5 0))'),
        ],
        crs=4326,
    )

    received = clean_overlapping_features(
        categories_gdf=land_object, land_uses_gdf=land_use, aoi=shapely.box(0, 0, 2.5, 2.5)
    )

    assert_geodataframe_equal(received, expected, check_like=True)


def test_clean_land_uses(simple_aoi):
    input_land_uses = gpd.GeoDataFrame(
        data={'land_use_category': [LandUseCategory.RESIDENTIAL, LandUseCategory.COMMERCIAL, LandUseCategory.UNKNOWN]},
        geometry=[shapely.box(0, 0, 0.5, 0.5), shapely.box(0.5, 0, 1.0, 0.5), shapely.box(0.5, 0.5, 1.0, 1.0)],
        crs=4326,
    )

    expected = gpd.GeoDataFrame(
        index=[LandUseCategory.COMMERCIAL, LandUseCategory.RESIDENTIAL, LandUseCategory.UNKNOWN],
        data={'priority': [0, 1, 9]},
        geometry=[shapely.box(0.5, 0, 1.0, 0.5), shapely.box(0, 0, 0.5, 0.5), shapely.box(0, 0.5, 1, 1)],
        crs=4326,
    )

    received = clean_land_uses(land_uses_gdf=input_land_uses, aoi=simple_aoi)

    assert_geodataframe_equal(received, expected, check_like=True)
