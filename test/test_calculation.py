from land_consumption.calculation import calculate_land_consumption


import geopandas as gpd
from land_consumption.utils import LandObjectCategory, LandUseCategory
from approvaltests import verify


def test_calculate_land_consumption():
    area_df = gpd.GeoDataFrame(
        {
            'category': [
                LandObjectCategory.BUILDINGS,
                LandObjectCategory.BUILT_UP,
                LandObjectCategory.BUILT_UP,
                LandObjectCategory.OTHER,
            ],
            'landuse_category': [
                LandUseCategory.COMMERCIAL,
                LandUseCategory.AGRICULTURAL,
                LandUseCategory.NATURAL,
                LandUseCategory.OTHER,
            ],
            'area': [4000, 3000, 500, 500],
        }
    )

    result = calculate_land_consumption(area_df)

    verify(result.to_json(indent=2))
