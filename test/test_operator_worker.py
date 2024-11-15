import pandas as pd

from land_consumption.operator_worker import LandConsumption


def test_get_table():
    data = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Unknown consumption'],
            'Total Land Area [ha]': [20, 80],
            '% Land Area': [20, 80],
            'Consumption Factor': [1, None],
            '% Land Consumed by known classes': [20, None],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Unknown consumption', 'Total'],
            'Total Land Area [ha]': [20, 80, 100],
            '% Land Area': [20, 80, 100],
            'Consumption Factor': [1, None, None],
            '% Land Consumed by known classes': [20, None, 20],
        }
    )

    received = LandConsumption.get_table(data)

    pd.testing.assert_frame_equal(received, expected_data)
