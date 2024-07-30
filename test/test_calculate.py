import pandas as pd

from land_consumption.calculate import calculate_land_consumption


def test_calculate_land_consumption():
    expected_df = pd.DataFrame(
        {
            'Land Use': ['Artificial surfaces', 'Agricultural areas', 'Forest and seminatural areas'],
            'Total Land Area [ha]': [20, 50, 30],
            '% Land Area': [20, 50, 30],
            'Consumption Factor': [1, 0.8, 0.33],
            '% Land Consumed': [20, 40, 10],
        }
    )

    result_df = calculate_land_consumption()

    pd.testing.assert_frame_equal(result_df, expected_df)
