import pandas as pd

from land_consumption.calculation import calculate_land_consumption


def test_calculate_land_consumption():
    expected_output = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Parking lots', 'Unknown consumption'],
            'Total Land Area [ha]': [0.25, 0.10, 0.65],
            '% Land Area': [25.0, 10.0, 65.0],
            'Consumption Factor': [1.0, 0.8, None],
            '% Land Consumed by known classes': [25.0, 8.0, None],
        }
    )

    computed_output = calculate_land_consumption(aoi_area=10000.0, building_area=2500, parking_area=1000)

    pd.testing.assert_frame_equal(computed_output, expected_output)
