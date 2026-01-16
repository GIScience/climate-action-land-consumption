import pandas as pd

from land_consumption.components.land_consumption import get_basic_table, get_detailed_table, create_treemap
from approvaltests import verify


def test_get_basic_table():
    data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Agricultural land', 'Natural land', 'Other', 'Unknown'],
            'Land Use Class': ['Commercial', '', '', 'Other land uses', 'Unknown land use'],
            '% of Consumed Land Area': [100.0, None, None, None, None],
            '% of Settled Land Area': [20.0, 10.0, None, 60.0, None],
            'Total Land Area [ha]': [20.0, 10.0, 10.0, 10.0, 50.0],
            '% of Total Land Area': [20.0, 10.0, 10.0, 10.0, 50.0],
        }
    )

    received = get_basic_table(data)

    verify(received.to_json(indent=2))


def test_get_detailed_table():
    data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Buildings', 'Agricultural land', 'Natural land', 'Other', 'Unknown'],
            'Land Use Class': [
                'Commercial',
                'Institutional',
                '',
                '',
                'Other',
                'Unknown land use',
            ],
            '% of Consumed Land Area': [50.0, 50.0, None, None, None, None],
            '% of Settled Land Area': [10.0, 10.0, 10.0, None, 60.0, None],
            'Total Land Area [ha]': [10.0, 10.0, 10.0, 10.0, 10.0, 50.0],
            '% of Total Land Area': [10.0, 10.0, 10.0, 10.0, 10.0, 50.0],
        }
    )

    received = get_detailed_table(data)

    verify(received.to_csv())


def test_get_basic_table_non_100():
    data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Parking lots', 'Other'],
            'Land Use Class': ['Commercial', 'Commercial', 'Other'],
            '% of Consumed Land Area': [66.67, 33.33, None],
            '% of Settled Land Area': [50.0, 25.0, 25.0],
            'Total Land Area [ha]': [2.0, 1.0, 1.0],
            '% of Total Land Area': [50.0, 25.0, 25.0],
        }
    )

    received = get_basic_table(data)

    verify(received.to_csv())


def test_get_detailed_table_non_100():
    data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Parking lots', 'Other'],
            'Land Use Class': ['Commercial', 'Commercial', 'Other'],
            '% of Consumed Land Area': [66.67, 33.33, None],
            '% of Settled Land Area': [50.0, 25.0, 25.0],
            'Total Land Area [ha]': [2.0, 1.0, 1.0],
            '% of Total Land Area': [50.0, 25.0, 25.0],
        }
    )

    received = get_detailed_table(data)

    verify(received.to_csv())


def test_table_sorted():
    data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Buildings'],
            'Land Use Class': ['Residential', 'Other land uses'],
            '% of Consumed Land Area': [66.67, 33.33],
            '% of Settled Land Area': [66.67, 33.33],
            'Total Land Area [ha]': [2.0, 1.0],
            '% of Total Land Area': [66.67, 33.33],
        }
    )

    received = get_detailed_table(data)

    verify(received.to_csv())


def test_get_treemap():
    data = pd.DataFrame(
        {
            'Land Use Object': ['Buildings', 'Building', 'Natural land', 'Other', 'Unknown'],
            'Land Use Class': ['Commercial', 'Industrial', '', 'Other', 'Unknown'],
            '% of Consumed Land Area': [66.67, 33.33, None, None, None],
            '% of Settled Land Area': [50.0, 25.0, None, 25.0, None],
            'Total Land Area [ha]': [2.0, 1.0, 1.0, 1.0, 5.0],
            '% of Total Land Area': [20.0, 10.0, 10.0, 10.0, 50.0],
        }
    )

    fig = create_treemap(data)

    assert fig.data[0].type == 'treemap'
    assert [
        'Land Use Overview',
        'Land Use Overview/Building/Industrial',
        'Land Use Overview/Building',
        'Land Use Overview/Buildings/Commercial',
        'Land Use Overview/Buildings',
        'Land Use Overview/Natural land/',
        'Land Use Overview/Natural land',
        'Land Use Overview/Other/Other',
        'Land Use Overview/Other',
        'Land Use Overview/Unknown/Unknown',
        'Land Use Overview/Unknown',
    ] in fig.data[0].ids
    assert [5.0, 1.0, 1.0, 2.0, 2.0, 1.0, 1.0, 1.0, 1.0, 5.0, 5.0] in fig.data[0].values
