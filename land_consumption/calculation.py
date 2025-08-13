import pandas as pd
from geopandas import GeoDataFrame

from land_consumption.utils import LandObjectCategory, SQM_TO_HA_FACTOR, LandUseCategory


def calculate_land_consumption(area_df: GeoDataFrame) -> pd.DataFrame:
    area_df['Total Land Area [ha]'] = area_df['area'] * SQM_TO_HA_FACTOR

    settled_land = area_df.apply(lambda x: x['area'] if is_land_settled(x) else None, axis='columns')
    area_df['% of Settled Land Area'] = settled_land / settled_land.sum() * 100

    consumed_land = area_df.apply(lambda x: x['area'] if is_land_consumed(x) else None, axis='columns')
    area_df['% of Consumed Land Area'] = consumed_land / consumed_land.sum() * 100

    area_df['Land Use Object'] = area_df['category'].apply(lambda x: x.value)
    area_df['Land Use Class'] = area_df['landuse_category'].apply(lambda x: x.value)

    mask = (area_df['category'] == LandObjectCategory.BUILT_UP) & (
        area_df['landuse_category'].isin([LandUseCategory.AGRICULTURAL, LandUseCategory.NATURAL])
    )

    area_df.loc[mask, 'Land Use Object'] = area_df.loc[mask, 'Land Use Class'].map(
        {
            'Agricultural': 'Agricultural land',
            'Natural': 'Natural land',
        }
    )
    area_df.loc[mask, 'Land Use Class'] = ''

    area_df['% of Total Land Area'] = area_df['area'] / area_df['area'].sum() * 100

    return area_df[
        [
            'Land Use Object',
            'Land Use Class',
            '% of Consumed Land Area',
            '% of Settled Land Area',
            'Total Land Area [ha]',
            '% of Total Land Area',
        ]
    ].round(2)


def is_land_consumed(feature: pd.Series) -> bool:
    if feature['category'] == LandObjectCategory.OTHER:
        return False
    elif (feature['category'] == LandObjectCategory.BUILT_UP) and (
        feature['landuse_category'] in [LandUseCategory.AGRICULTURAL, LandUseCategory.NATURAL]
    ):
        return False

    return True


def is_land_settled(feature: pd.Series) -> bool:
    # TODO the naming here makes this line needlessly confusing fix this in review
    if (feature['category'] == LandObjectCategory.BUILT_UP) and (
        feature['landuse_category'] == LandUseCategory.NATURAL
    ):
        return False
    return True
