import pandas as pd

from land_consumption.operator_worker import LandConsumption


def test_get_table():
    data = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Parking lots', 'Unknown consumption'],
            'Total Land Area [ha]': [20.0, 10.0, 70.0],
            '% Land Area': [20.0, 10.0, 70.0],
            'Consumption Factor': [1.0, 0.8, None],
            '% Land Consumed by known classes': [20.0, 8.0, None],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Parking lots', 'Unknown consumption', 'Total'],
            'Total Land Area [ha]': [20.0, 10.0, 70.0, 100.0],
            '% Land Area': [20.0, 10.0, 70.0, 100.0],
            'Consumption Factor': [1.0, 0.8, None, None],
            '% Land Consumed by known classes': [20.0, 8.0, None, 28.0],
        }
    )

    received = LandConsumption.get_table(data)

    pd.testing.assert_frame_equal(received, expected_data)


def test_get_table_non_100():
    data = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Parking lots', 'Unknown consumption'],
            'Total Land Area [ha]': [2.0, 1.0, 1.0],
            '% Land Area': [50.0, 25.0, 25.0],
            'Consumption Factor': [1.0, 0.8, None],
            '% Land Consumed by known classes': [50.0, 20.0, None],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use': ['Buildings', 'Parking lots', 'Unknown consumption', 'Total'],
            'Total Land Area [ha]': [2.0, 1.0, 1.0, 4.0],
            '% Land Area': [50.0, 25.0, 25.0, 100.0],
            'Consumption Factor': [1.0, 0.8, None, None],
            '% Land Consumed by known classes': [50.0, 20.0, None, 70.0],
        }
    )

    received = LandConsumption.get_table(data)

    pd.testing.assert_frame_equal(received, expected_data)
