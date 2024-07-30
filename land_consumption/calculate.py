import pandas as pd


def calculate_land_consumption() -> pd.DataFrame:
    land_consumption_df = pd.DataFrame(
        {
            'Land Use': ['Artificial surfaces', 'Agricultural areas', 'Forest and seminatural areas'],
            'Total Land Area [ha]': [20, 50, 30],
            '% Land Area': [20, 50, 30],
            'Consumption Factor': [1, 0.8, 0.33],
            '% Land Consumed': [20, 40, 10],
        }
    )

    return land_consumption_df
