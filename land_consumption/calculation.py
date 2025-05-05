import pandas as pd
from geopandas import GeoDataFrame

from land_consumption.utils import LandObjectCategory, SQM_TO_HA_FACTOR, LandUseCategory


def calculate_land_consumption(aoi_area: float, area_df: GeoDataFrame) -> pd.DataFrame:
    area_df['Total Land Area [ha]'] = area_df['area'] * SQM_TO_HA_FACTOR
    area_df['% of Consumed Land Area'] = round(
        area_df['area'] / area_df[area_df['category'] != 'OTHER']['area'].sum() * 100, 2
    )
    area_df.loc[area_df['category'] == 'OTHER', '% of Consumed Land Area'] = None
    aoi_area_ha = aoi_area * SQM_TO_HA_FACTOR

    area_df['% Land Area'] = (area_df['Total Land Area [ha]'] / aoi_area_ha) * 100

    area_df['Land Use Object'] = area_df['category'].apply(lambda x: LandObjectCategory[x].value)
    area_df['Land Use Class'] = area_df['landuse_category'].apply(lambda x: LandUseCategory[x].value)

    return area_df[
        [
            'Land Use Object',
            'Land Use Class',
            'Total Land Area [ha]',
            '% of Consumed Land Area',
            '% Land Area',
        ]
    ]
