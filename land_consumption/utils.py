import logging
from enum import Enum

import geopandas as gpd
import shapely
from ohsome import OhsomeClient

log = logging.getLogger(__name__)

SQM_TO_HA_FACTOR = 1.0 / (100.0 * 100.0)


class LandUseCategory(Enum):
    BUILDINGS = 'Buildings'
    PARKING_LOTS = 'Parking lots'
    UNKNOWN = 'Unknown consumption'


# arbitrary, could e user input or flexible
CONSUMPTION_FACTOR_LOOKUP = {
    LandUseCategory.BUILDINGS: 1.0,
    LandUseCategory.PARKING_LOTS: 0.8,
    LandUseCategory.UNKNOWN: None,
}


def fetch_osm_area(aoi: shapely.MultiPolygon, osm_filter: str, ohsome: OhsomeClient) -> float:
    area = ohsome.elements.area.post(bpolys=aoi, filter=osm_filter).as_dataframe()
    return area.value.sum()


def get_ohsome_filter(category: LandUseCategory) -> str:
    match category:
        case LandUseCategory.BUILDINGS:
            return 'geometry:polygon and building=* and not building=no'
        case LandUseCategory.PARKING_LOTS:
            return 'geometry:polygon and amenity=parking and parking=surface'
        case _:
            raise ValueError(f'{category} does not have an osm filter')


def calculate_area(gdf_input: gpd.GeoSeries) -> float:
    if gdf_input.empty:
        return 0.0
    projected_input = gdf_input.to_crs(gdf_input.estimate_utm_crs())
    return projected_input.area.sum()
