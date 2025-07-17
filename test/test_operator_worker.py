import pandas as pd

from land_consumption.operator_worker import LandConsumption


def test_get_basic_table():
    data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Agricultural land', 'Other'],
            'Land Use Class': ['Commercial', 'Agricultural', 'Other land uses'],
            'Total Land Area [ha]': [20.0, 10.0, 70.0],
            '% of Consumed Land Area': [66.67, 33.33, None],
            '% Land Area': [20.0, 10.0, 70.0],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Object': pd.Categorical(
                ['Buildings', 'Agricultural land', 'Other', 'Total'],
                categories=[
                    'Buildings',
                    'Built up land',
                    'Parking lots',
                    'Roads',
                    'Agricultural land',
                    'Other',
                    'Total',
                ],
                ordered=True,
            ),
            'Total Land Area [ha]': [20.0, 10.0, 70.0, 100.0],
            '% of Consumed Land Area': [66.67, 33.33, None, 100.0],
            '% Land Area': [20.0, 10.0, 70.0, 100.0],
        }
    )
    expected_data.set_index('Land Use Object', inplace=True)

    received = LandConsumption.get_basic_table(data)

    pd.testing.assert_frame_equal(received, expected_data)


def test_get_detailed_table():
    data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Buildings', 'Agricultural land', 'Other'],
            'Land Use Class': [
                'Commercial',
                'Industrial',
                '',
                'Other',
            ],
            'Total Land Area [ha]': [10.0, 10.0, 10.0, 70.0],
            '% of Consumed Land Area': [33.33, 33.33, 33.33, None],
            '% Land Area': [10.0, 10.0, 10.0, 70.0],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', '', '', 'Agricultural land', 'Other', 'Total'],
            'Land Use Class': [
                'Commercial',
                'Industrial',
                'Subtotal',
                '',
                'Other',
                '',
            ],
            'Total Land Area [ha]': [10.0, 10.0, 20.0, 10.0, 70.0, 100.0],
            '% of Consumed Land Area': [33.33, 33.33, 66.66, 33.33, None, 100.0],
            '% Land Area': [10.0, 10.0, 20.0, 10.0, 70.0, 100.0],
        }
    )
    expected_data.set_index('Land Use Object', inplace=True)

    received = LandConsumption.get_detailed_table(data)

    pd.testing.assert_frame_equal(received, expected_data)


def test_get_basic_table_non_100():
    data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Parking lots', 'Other'],
            'Land Use Class': ['Commercial', 'Commercial', 'Other'],
            'Total Land Area [ha]': [2.0, 1.0, 1.0],
            '% of Consumed Land Area': [66.67, 33.33, None],
            '% Land Area': [50.0, 25.0, 25.0],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Object': pd.Categorical(
                ['Buildings', 'Parking lots', 'Other', 'Total'],
                categories=[
                    'Buildings',
                    'Built up land',
                    'Parking lots',
                    'Roads',
                    'Agricultural land',
                    'Other',
                    'Total',
                ],
                ordered=True,
            ),
            'Total Land Area [ha]': [2.0, 1.0, 1.0, 4.0],
            '% of Consumed Land Area': [66.67, 33.33, None, 100.0],
            '% Land Area': [50.0, 25.0, 25.0, 100.0],
        }
    )
    expected_data.set_index('Land Use Object', inplace=True)

    received = LandConsumption.get_basic_table(data)

    pd.testing.assert_frame_equal(received, expected_data)


def test_get_detailed_table_non_100():
    data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Parking lots', 'Other'],
            'Land Use Class': ['Commercial', 'Commercial', 'Other'],
            'Total Land Area [ha]': [2.0, 1.0, 1.0],
            '% of Consumed Land Area': [66.67, 33.33, None],
            '% Land Area': [50.0, 25.0, 25.0],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', '', 'Parking lots', '', 'Other', 'Total'],
            'Land Use Class': [
                'Commercial',
                'Subtotal',
                'Commercial',
                'Subtotal',
                'Other',
                '',
            ],
            'Total Land Area [ha]': [2.0, 2.0, 1.0, 1.0, 1.0, 4.0],
            '% of Consumed Land Area': [66.67, 66.67, 33.33, 33.33, None, 100.0],
            '% Land Area': [50.0, 50.0, 25.0, 25.0, 25.0, 100.0],
        }
    )
    expected_data.set_index('Land Use Object', inplace=True)

    received = LandConsumption.get_detailed_table(data)

    pd.testing.assert_frame_equal(received, expected_data)


def test_table_sorted():
    data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Buildings'],
            'Land Use Class': ['Residential', 'Other land uses'],
            'Total Land Area [ha]': [2.0, 1.0],
            '% of Consumed Land Area': [66.67, 33.33],
            '% Land Area': [66.67, 33.33],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', '', '', 'Total'],
            'Land Use Class': [
                'Residential',
                'Other land uses',
                'Subtotal',
                '',
            ],
            'Total Land Area [ha]': [2.0, 1.0, 3.0, 3.0],
            '% of Consumed Land Area': [66.67, 33.33, 100.0, 100.0],
            '% Land Area': [66.67, 33.33, 100.0, 100.0],
        }
    )
    expected_data.set_index('Land Use Object', inplace=True)

    received = LandConsumption.get_detailed_table(data)

    pd.testing.assert_frame_equal(received, expected_data)
