import numpy as np

from land_consumption.calculation import calculate_land_consumption


import pytest
import geopandas as gpd
from land_consumption.utils import LandUseCategory, CONSUMPTION_FACTOR_LOOKUP, SQM_TO_HA_FACTOR


def test_calculate_land_consumption():
    aoi_area = 10000
    area_df = gpd.GeoDataFrame(
        {
            'category': ['BUILDINGS', 'PARKING_LOTS', 'PAVED_ROADS', 'BUILT_UP'],
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

    unknown_row = result[result['Land Use'] == 'Unknown consumption']
    assert not unknown_row.empty
    assert unknown_row['Total Land Area [ha]'].iloc[0] == pytest.approx((10000 - 9500) * SQM_TO_HA_FACTOR)

    assert result.loc[0, 'Consumption Factor'] == CONSUMPTION_FACTOR_LOOKUP[LandUseCategory.BUILDINGS]
    assert np.isnan(unknown_row['Consumption Factor'].iloc[0])

    assert result.loc[0, '% Land Consumed by known classes'] == pytest.approx(
        result.loc[0, '% Land Area'] * result.loc[0, 'Consumption Factor']
    )
    assert result.loc[1, '% Land Consumed by known classes'] == pytest.approx(
        result.loc[1, '% Land Area'] * result.loc[1, 'Consumption Factor']
    )
    assert np.isnan(unknown_row['% Land Consumed by known classes'].iloc[0])


def test_calculate_land_consumption_empty():
    aoi_area = 10000
    area_df = gpd.GeoDataFrame(columns=['category', 'area'])

    result = calculate_land_consumption(aoi_area, area_df)

    assert len(result) == 1
    assert result['Land Use'].iloc[0] == 'Unknown consumption'
    assert result['Total Land Area [ha]'].iloc[0] == pytest.approx(10000 * SQM_TO_HA_FACTOR)


def test_consumption_factor_lookup():
    for category in LandUseCategory:
        if category != LandUseCategory.UNKNOWN:
            assert CONSUMPTION_FACTOR_LOOKUP[category] is not None
        else:
            assert CONSUMPTION_FACTOR_LOOKUP[category] is None
