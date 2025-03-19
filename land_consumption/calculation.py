import pandas as pd
from geopandas import GeoDataFrame

from land_consumption.utils import LandObjectCategory, CONSUMPTION_FACTOR_LOOKUP, SQM_TO_HA_FACTOR, LandUseCategory


def calculate_land_consumption(aoi_area: float, area_df: GeoDataFrame) -> pd.DataFrame:
    area_df['Total Land Area [ha]'] = area_df['area'] * SQM_TO_HA_FACTOR
    aoi_area_ha = aoi_area * SQM_TO_HA_FACTOR

    area_df['% Land Area'] = (area_df['Total Land Area [ha]'] / aoi_area_ha) * 100

    area_df['Consumption Factor'] = [
        CONSUMPTION_FACTOR_LOOKUP.get(LandObjectCategory[x], None) for x in area_df['category']
    ]

    area_df['Land Use Objects'] = area_df['category'].apply(lambda x: LandObjectCategory[x].value)
    area_df['Land Use Class'] = area_df['landuse_category'].apply(lambda x: LandUseCategory[x].value)

    area_df['% Land Consumed by known classes'] = area_df['% Land Area'] * area_df['Consumption Factor']

    return area_df[
        [
            'Land Use Objects',
            'Land Use Class',
            'Total Land Area [ha]',
            '% Land Area',
            'Consumption Factor',
            '% Land Consumed by known classes',
        ]
    ]
