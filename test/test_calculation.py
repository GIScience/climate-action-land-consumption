import pandas as pd

from land_consumption.calculation import calculate_land_consumption


def test_calculate_land_consumption():
    expected_output = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Unknown consumption'],
            'Total Land Area [ha]': [0.25, 0.75],
            '% Land Area': [25.0, 75.0],
            'Consumption Factor': [1.0, None],
            '% Land Consumed by known classes': [25.0, None],
        }
    )

    computed_output = calculate_land_consumption(aoi_area=1.0, building_area=0.25)

    pd.testing.assert_frame_equal(computed_output, expected_output)
