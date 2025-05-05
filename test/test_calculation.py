import pandas as pd

from land_consumption.calculation import calculate_land_consumption


import pytest
import geopandas as gpd
from land_consumption.utils import SQM_TO_HA_FACTOR


def test_calculate_land_consumption():
    aoi_area = 10000
    area_df = gpd.GeoDataFrame(
        {
            'category': ['BUILDINGS', 'PARKING_LOTS', 'ROADS', 'BUILT_UP'],
            'landuse_category': ['COMMERCIAL', 'COMMERCIAL', 'COMMERCIAL', 'COMMERCIAL'],
            'area': [4000, 3000, 2000, 500],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Parking lots', 'Roads', 'Built up land'],
            'Land Use Class': ['Commercial', 'Commercial', 'Commercial', 'Commercial'],
            'Total Land Area [ha]': [0.4, 0.3, 0.2, 0.05],
            '% of Consumed Land Area': [42.11, 31.58, 21.05, 5.26],
            '% Land Area': [40.0, 30.0, 20.0, 5.0],
        }
    )

    result = calculate_land_consumption(aoi_area, area_df)

    assert 'Total Land Area [ha]' in result.columns
    assert '% Land Area' in result.columns
    assert '% of Consumed Land Area' in result.columns

    assert result.loc[0, 'Total Land Area [ha]'] == 4000 * SQM_TO_HA_FACTOR

    assert result.loc[0, '% Land Area'] == pytest.approx(
        (4000 * SQM_TO_HA_FACTOR) / (aoi_area * SQM_TO_HA_FACTOR) * 100
    )

    pd.testing.assert_frame_equal(result, expected_data)
