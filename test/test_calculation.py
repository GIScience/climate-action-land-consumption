from land_consumption.calculation import calculate_land_consumption


import pytest
import geopandas as gpd
from land_consumption.utils import LandObjectCategory, CONSUMPTION_FACTOR_LOOKUP, SQM_TO_HA_FACTOR


def test_calculate_land_consumption():
    aoi_area = 10000
    area_df = gpd.GeoDataFrame(
        {
            'category': ['BUILDINGS', 'PARKING_LOTS', 'ROADS', 'BUILT_UP'],
            'landuse_category': ['COMMERCIAL', 'COMMERCIAL', 'COMMERCIAL', 'COMMERCIAL'],
            'area': [4000, 3000, 2000, 500],
        }
    )

    result = calculate_land_consumption(aoi_area, area_df)

    assert 'Total Land Area [ha]' in result.columns
    assert '% Land Area' in result.columns
    assert 'Consumption Factor' in result.columns
    assert '% Land Consumed by known classes' in result.columns

    assert result.loc[0, 'Total Land Area [ha]'] == 4000 * SQM_TO_HA_FACTOR

    assert result.loc[0, '% Land Area'] == pytest.approx(
        (4000 * SQM_TO_HA_FACTOR) / (aoi_area * SQM_TO_HA_FACTOR) * 100
    )

    assert result.loc[0, 'Consumption Factor'] == CONSUMPTION_FACTOR_LOOKUP[LandObjectCategory.BUILDINGS]

    assert result.loc[0, '% Land Consumed by known classes'] == pytest.approx(
        result.loc[0, '% Land Area'] * result.loc[0, 'Consumption Factor']
    )
    assert result.loc[1, '% Land Consumed by known classes'] == pytest.approx(
        result.loc[1, '% Land Area'] * result.loc[1, 'Consumption Factor']
    )


def test_consumption_factor_lookup():
    for category in LandObjectCategory:
        if category != LandObjectCategory.UNKNOWN:
            assert CONSUMPTION_FACTOR_LOOKUP[category] is not None
        else:
            assert CONSUMPTION_FACTOR_LOOKUP[category] is None
