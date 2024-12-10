import pandas as pd
from geopandas import GeoDataFrame

from land_consumption.utils import LandUseCategory, CONSUMPTION_FACTOR_LOOKUP, SQM_TO_HA_FACTOR


def calculate_land_consumption(aoi_area: float, area_df: GeoDataFrame) -> pd.DataFrame:
    area_df['Total Land Area [ha]'] = area_df['area'] * SQM_TO_HA_FACTOR
    aoi_area_ha = aoi_area * SQM_TO_HA_FACTOR

    area_df['% Land Area'] = (area_df['Total Land Area [ha]'] / aoi_area_ha) * 100

    # Calculate the used and unknown areas
    used_area_ha = area_df['Total Land Area [ha]'].sum()
    unknown_area_ha = aoi_area_ha - used_area_ha
    unknown_percent = 100 - area_df['% Land Area'].sum()

    # Add unknown category
    area_df = pd.concat(
        [
            area_df,
            pd.DataFrame(
                {
                    'category': [LandUseCategory.UNKNOWN.name],
                    'area': [unknown_area_ha / SQM_TO_HA_FACTOR],
                    'Total Land Area [ha]': [unknown_area_ha],
                    '% Land Area': [unknown_percent],
                }
            ),
        ],
        ignore_index=True,
    )

    # Map consumption factors
    area_df['Consumption Factor'] = [
        CONSUMPTION_FACTOR_LOOKUP.get(LandUseCategory[x], None) for x in area_df['category']
    ]

    area_df['Land Use'] = area_df['category'].apply(lambda x: LandUseCategory[x].value)

    # Calculate land consumed
    area_df['% Land Consumed by known classes'] = area_df['% Land Area'] * area_df['Consumption Factor']

    return area_df[
        ['Land Use', 'Total Land Area [ha]', '% Land Area', 'Consumption Factor', '% Land Consumed by known classes']
    ]
