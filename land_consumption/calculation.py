import pandas as pd

from land_consumption.utils import LandUseCategory, CONSUMPTION_FACTOR_LOOKUP, SQM_TO_HA_FACTOR


def calculate_land_consumption(aoi_area: float, building_area: float) -> pd.DataFrame:
    building_area = building_area * SQM_TO_HA_FACTOR
    aoi_area = aoi_area * SQM_TO_HA_FACTOR
    building_proportion = building_area / aoi_area

    table = pd.DataFrame(
        {
            'Land Use': [LandUseCategory.BUILDINGS.value, LandUseCategory.UNKNOWN.value],
            'Total Land Area [ha]': [building_area, aoi_area - building_area],
            '% Land Area': [building_proportion * 100.0, (1 - building_proportion) * 100.0],
            'Consumption Factor': [
                CONSUMPTION_FACTOR_LOOKUP.get(LandUseCategory.BUILDINGS),
                CONSUMPTION_FACTOR_LOOKUP.get(LandUseCategory.UNKNOWN),
            ],
        }
    )
    consumption = table['Consumption Factor'] * table['% Land Area']
    table['% Land Consumed by known classes'] = consumption

    return table
