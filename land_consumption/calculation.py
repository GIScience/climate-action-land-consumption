import pandas as pd
from geopandas import GeoDataFrame

from land_consumption.utils import LandObjectCategory, SQM_TO_HA_FACTOR, LandUseCategory


def calculate_land_consumption(aoi_area: float, area_df: GeoDataFrame) -> pd.DataFrame:
    area_df['Total Land Area [ha]'] = area_df['area'] * SQM_TO_HA_FACTOR
    aoi_area_ha = aoi_area * SQM_TO_HA_FACTOR
    area_df['% of Settled Land Area'] = (area_df['Total Land Area [ha]'] / aoi_area_ha) * 100

    consumed_land = area_df.apply(lambda x: x['area'] if is_land_consumed(x) else None, axis='columns')
    area_df['% of Consumed Land Area'] = consumed_land / consumed_land.sum() * 100

    area_df['Land Use Object'] = area_df['category'].apply(lambda x: x.value)
    area_df['Land Use Class'] = area_df['landuse_category'].apply(lambda x: x.value)

    agri_mask = (area_df['Land Use Object'] == 'Built up land') & (area_df['Land Use Class'] == 'Agricultural')
    area_df.loc[agri_mask, 'Land Use Object'] = 'Agricultural land'
    area_df.loc[agri_mask, 'Land Use Class'] = ''

    return area_df[
        [
            'Land Use Object',
            'Land Use Class',
            'Total Land Area [ha]',
            '% of Consumed Land Area',
            '% of Settled Land Area',
        ]
    ].round(2)


def is_land_consumed(feature: pd.Series) -> bool:
    if feature['category'] == LandObjectCategory.OTHER:
        return False
    elif (feature['category'] == LandObjectCategory.BUILT_UP) & (
        feature['landuse_category'] == LandUseCategory.AGRICULTURAL
    ):
        return False
    return True
