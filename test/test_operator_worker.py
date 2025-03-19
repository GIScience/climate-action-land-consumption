import pandas as pd

from land_consumption.operator_worker import LandConsumption


def test_get_table():
    data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', 'Parking lots', 'Unknown'],
            'Land Use Class': ['Commercial built up area', 'Commercial built up area', 'Unknown'],
            'Total Land Area [ha]': [20.0, 10.0, 70.0],
            '% Land Area': [20.0, 10.0, 70.0],
            'Consumption Factor': [1.0, 0.8, None],
            '% Land Consumed by known classes': [20.0, 8.0, None],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', '', 'Parking lots', '', 'Unknown', 'Total'],
            'Land Use Class': [
                'Commercial built up area',
                'Subtotal',
                'Commercial built up area',
                'Subtotal',
                'Unknown',
                '',
            ],
            'Total Land Area [ha]': [20.0, 20.0, 10.0, 10.0, 70.0, 100.0],
            '% Land Area': [20.0, 20.0, 10.0, 10.0, 70.0, 100.0],
            'Consumption Factor': [1.0, 1.0, 0.8, 0.8, None, None],
            '% Land Consumed by known classes': [20.0, 20.0, 8.0, 8.0, None, 28.0],
        }
    )
    expected_data.set_index('Land Use Objects', inplace=True)

    received = LandConsumption.get_table(data)

    pd.testing.assert_frame_equal(received, expected_data)


def test_get_table_non_100():
    data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', 'Parking lots', 'Unknown'],
            'Land Use Class': ['Commercial built up area', 'Commercial built up area', 'Unknown'],
            'Total Land Area [ha]': [2.0, 1.0, 1.0],
            '% Land Area': [50.0, 25.0, 25.0],
            'Consumption Factor': [1.0, 0.8, None],
            '% Land Consumed by known classes': [50.0, 20.0, None],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', '', 'Parking lots', '', 'Unknown', 'Total'],
            'Land Use Class': [
                'Commercial built up area',
                'Subtotal',
                'Commercial built up area',
                'Subtotal',
                'Unknown',
                '',
            ],
            'Total Land Area [ha]': [2.0, 2.0, 1.0, 1.0, 1.0, 4.0],
            '% Land Area': [50.0, 50.0, 25.0, 25.0, 25.0, 100.0],
            'Consumption Factor': [1.0, 1.0, 0.8, 0.8, None, None],
            '% Land Consumed by known classes': [50.0, 50.0, 20.0, 20.0, None, 70.0],
        }
    )
    expected_data.set_index('Land Use Objects', inplace=True)

    received = LandConsumption.get_table(data)

    pd.testing.assert_frame_equal(received, expected_data)
