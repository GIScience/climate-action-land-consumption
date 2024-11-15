import pandas as pd

from land_consumption.operator_worker import LandConsumption


def test_get_table():
    data = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Unknown consumption'],
            'Total Land Area [ha]': [20.0, 80.0],
            '% Land Area': [20.0, 80.0],
            'Consumption Factor': [1.0, None],
            '% Land Consumed by known classes': [20.0, None],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Unknown consumption', 'Total'],
            'Total Land Area [ha]': [20.0, 80.0, 100.0],
            '% Land Area': [20.0, 80.0, 100.0],
            'Consumption Factor': [1.0, None, None],
            '% Land Consumed by known classes': [20.0, None, 20.0],
        }
    )

    received = LandConsumption.get_table(data)

    pd.testing.assert_frame_equal(received, expected_data)


def test_get_table_non_100():
    data = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Unknown consumption'],
            'Total Land Area [ha]': [3.0, 1.0],
            '% Land Area': [75.0, 25.0],
            'Consumption Factor': [1.0, None],
            '% Land Consumed by known classes': [75.0, None],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Unknown consumption', 'Total'],
            'Total Land Area [ha]': [3.0, 1.0, 4.0],
            '% Land Area': [75.0, 25.0, 100.0],
            'Consumption Factor': [1.0, None, None],
            '% Land Consumed by known classes': [75.0, None, 75.0],
        }
    )

    received = LandConsumption.get_table(data)

    pd.testing.assert_frame_equal(received, expected_data)
