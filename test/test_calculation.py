import pandas as pd

from land_consumption.calculation import calculate_land_consumption


import pytest
import geopandas as gpd
from land_consumption.utils import SQM_TO_HA_FACTOR, LandObjectCategory, LandUseCategory


def test_calculate_land_consumption():
    aoi_area = 10000
    area_df = gpd.GeoDataFrame(
        {
            'category': [
                LandObjectCategory.BUILDINGS,
                LandObjectCategory.BUILT_UP,
                LandObjectCategory.OTHER,
            ],
            'landuse_category': [
                LandUseCategory.COMMERCIAL,
                LandUseCategory.AGRICULTURAL,
                LandUseCategory.OTHER,
            ],
            'area': [4000, 3000, 500],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Agricultural land', 'Other'],
            'Land Use Class': ['Commercial', '', 'Other land uses'],
            'Total Land Area [ha]': [0.4, 0.3, 0.05],
            '% of Consumed Land Area': [100, None, None],
            '% Settled Land Area': [40.0, 30.0, 5.0],
        }
    )

    result = calculate_land_consumption(aoi_area, area_df)

    assert 'Total Land Area [ha]' in result.columns
    assert '% Settled Land Area' in result.columns
    assert '% of Consumed Land Area' in result.columns

    assert result.loc[0, 'Total Land Area [ha]'] == 4000 * SQM_TO_HA_FACTOR

    assert result.loc[0, '% Settled Land Area'] == pytest.approx(
        (4000 * SQM_TO_HA_FACTOR) / (aoi_area * SQM_TO_HA_FACTOR) * 100
    )

    pd.testing.assert_frame_equal(result, expected_data)
