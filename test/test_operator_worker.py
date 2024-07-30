import pandas as pd

from land_consumption.operator_worker import LandConsumption


def test_get_table():
    data = pd.DataFrame(
        {
            'Land Use': ['Artificial surfaces', 'Agricultural areas', 'Forest and seminatural areas'],
            'Total Land Area [ha]': [20, 50, 30],
            '% Land Area': [20, 50, 30],
            'Consumption Factor': [1, 0.8, 0.33],
            '% Land Consumed': [20, 40, 10],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use': [
                'Artificial surfaces',
                'Agricultural areas',
                'Forest and seminatural areas',
                'Total',
            ],
            'Total Land Area [ha]': [20, 50, 30, 100],
            '% Land Area': [20, 50, 30, 100],
            'Consumption Factor': [1, 0.8, 0.33, None],
            '% Land Consumed': [20, 40, 10, 70],
        }
    )

    received = LandConsumption.get_table(data)

    pd.testing.assert_frame_equal(received, expected_data)
