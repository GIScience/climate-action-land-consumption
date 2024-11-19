import pandas as pd

from land_consumption.utils import LandUseCategory, CONSUMPTION_FACTOR_LOOKUP, SQM_TO_HA_FACTOR


def calculate_land_consumption(aoi_area: float, building_area: float, parking_area: float) -> pd.DataFrame:
    building_area = building_area * SQM_TO_HA_FACTOR
    parking_area = parking_area * SQM_TO_HA_FACTOR
    aoi_area = aoi_area * SQM_TO_HA_FACTOR
    used_area = building_area + parking_area

    building_percent = (building_area / aoi_area) * 100.0
    parking_percent = (parking_area / aoi_area) * 100.0
    used_percent = building_percent + parking_percent

    table = pd.DataFrame(
        {
            'Land Use': [
                LandUseCategory.BUILDINGS.value,
                LandUseCategory.PARKING_LOTS.value,
                LandUseCategory.UNKNOWN.value,
            ],
            'Total Land Area [ha]': [building_area, parking_area, aoi_area - used_area],
            '% Land Area': [building_percent, parking_percent, 100.0 - used_percent],
            'Consumption Factor': [
                CONSUMPTION_FACTOR_LOOKUP.get(LandUseCategory.BUILDINGS),
                CONSUMPTION_FACTOR_LOOKUP.get(LandUseCategory.PARKING_LOTS),
                CONSUMPTION_FACTOR_LOOKUP.get(LandUseCategory.UNKNOWN),
            ],
        }
    )
    consumption = table['Consumption Factor'] * table['% Land Area']
    table['% Land Consumed by known classes'] = consumption

    return table
