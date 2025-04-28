import pandas as pd

from land_consumption.operator_worker import LandConsumption


def test_get_basic_table():
    data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', 'Parking lots', 'Other'],
            'Land Use Class': ['Commercial built up area', 'Commercial built up area', 'Other land uses'],
            'Total Land Area [ha]': [20.0, 10.0, 70.0],
            '% of Consumed Land Area': [66.67, 33.33, None],
            '% Land Area': [20.0, 10.0, 70.0],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', 'Parking lots', 'Other', 'Total'],
            'Total Land Area [ha]': [20.0, 10.0, 70.0, 100.0],
            '% of Consumed Land Area': [66.67, 33.33, None, 100.0],
            '% Land Area': [20.0, 10.0, 70.0, 100.0],
        }
    )
    expected_data.set_index('Land Use Objects', inplace=True)

    received = LandConsumption.get_basic_table(data)

    pd.testing.assert_frame_equal(received, expected_data)


def test_get_detailed_table():
    data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', 'Buildings', 'Parking lots', 'Other'],
            'Land Use Class': [
                'Commercial built up area',
                'Industrial built up area',
                'Commercial built up area',
                'Other',
            ],
            'Total Land Area [ha]': [10.0, 10.0, 10.0, 70.0],
            '% of Consumed Land Area': [33.33, 33.33, 33.33, None],
            '% Land Area': [10.0, 10.0, 10.0, 70.0],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', '', '', 'Parking lots', '', 'Other', 'Total'],
            'Land Use Class': [
                'Commercial built up area',
                'Industrial built up area',
                'Subtotal',
                'Commercial built up area',
                'Subtotal',
                'Other',
                '',
            ],
            'Total Land Area [ha]': [10.0, 10.0, 20.0, 10.0, 10.0, 70.0, 100.0],
            '% of Consumed Land Area': [33.33, 33.33, 66.66, 33.33, 33.33, None, 100.0],
            '% Land Area': [10.0, 10.0, 20.0, 10.0, 10.0, 70.0, 100.0],
        }
    )
    expected_data.set_index('Land Use Objects', inplace=True)

    received = LandConsumption.get_detailed_table(data)

    pd.testing.assert_frame_equal(received, expected_data)


def test_get_basic_table_non_100():
    data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', 'Parking lots', 'Other'],
            'Land Use Class': ['Commercial built up area', 'Commercial built up area', 'Other'],
            'Total Land Area [ha]': [2.0, 1.0, 1.0],
            '% of Consumed Land Area': [66.67, 33.33, None],
            '% Land Area': [50.0, 25.0, 25.0],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', 'Parking lots', 'Other', 'Total'],
            'Total Land Area [ha]': [2.0, 1.0, 1.0, 4.0],
            '% of Consumed Land Area': [66.67, 33.33, None, 100.0],
            '% Land Area': [50.0, 25.0, 25.0, 100.0],
        }
    )
    expected_data.set_index('Land Use Objects', inplace=True)

    received = LandConsumption.get_basic_table(data)

    pd.testing.assert_frame_equal(received, expected_data)


def test_get_detailed_table_non_100():
    data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', 'Parking lots', 'Other'],
            'Land Use Class': ['Commercial built up area', 'Commercial built up area', 'Other'],
            'Total Land Area [ha]': [2.0, 1.0, 1.0],
            '% of Consumed Land Area': [66.67, 33.33, None],
            '% Land Area': [50.0, 25.0, 25.0],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', '', 'Parking lots', '', 'Other', 'Total'],
            'Land Use Class': [
                'Commercial built up area',
                'Subtotal',
                'Commercial built up area',
                'Subtotal',
                'Other',
                '',
            ],
            'Total Land Area [ha]': [2.0, 2.0, 1.0, 1.0, 1.0, 4.0],
            '% of Consumed Land Area': [66.67, 66.67, 33.33, 33.33, None, 100.0],
            '% Land Area': [50.0, 50.0, 25.0, 25.0, 25.0, 100.0],
        }
    )
    expected_data.set_index('Land Use Objects', inplace=True)

    received = LandConsumption.get_detailed_table(data)

    pd.testing.assert_frame_equal(received, expected_data)


def test_table_sorted():
    data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', 'Buildings'],
            'Land Use Class': ['Residential built up area', 'Other land uses'],
            'Total Land Area [ha]': [2.0, 1.0],
            '% of Consumed Land Area': [66.67, 33.33],
            '% Land Area': [66.67, 33.33],
        }
    )

    expected_data = pd.DataFrame(
        {
            'Land Use Objects': ['Buildings', '', '', 'Total'],
            'Land Use Class': [
                'Residential built up area',
                'Other land uses',
                'Subtotal',
                '',
            ],
            'Total Land Area [ha]': [2.0, 1.0, 3.0, 3.0],
            '% of Consumed Land Area': [66.67, 33.33, 100.0, 100.0],
            '% Land Area': [66.67, 33.33, 100.0, 100.0],
        }
    )
    expected_data.set_index('Land Use Objects', inplace=True)

    received = LandConsumption.get_detailed_table(data)

    pd.testing.assert_frame_equal(received, expected_data)
