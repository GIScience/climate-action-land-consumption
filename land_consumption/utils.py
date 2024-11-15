import logging
from enum import Enum

import geopandas as gpd
import shapely
from ohsome import OhsomeClient

log = logging.getLogger(__name__)


class LandUseCategory(Enum):
    BUILDINGS = 'Buildings'
    UNKNOWN = 'Unknown consumption'


# arbitrary, could e user input or flexible
CONSUMPTION_FACTOR_LOOKUP = {LandUseCategory.BUILDINGS: 1.0, LandUseCategory.UNKNOWN: None}


def fetch_osm_area(aoi: shapely.MultiPolygon, osm_filter: str, ohsome: OhsomeClient) -> gpd.GeoDataFrame:
    area = ohsome.elements.area.post(bpolys=aoi, filter=osm_filter).as_dataframe()
    area = area.reset_index(drop=True)
    area = area.rename(columns={'value': 'area'})
    return area[['area']]


def get_ohsome_filter(category: LandUseCategory) -> str:
    match category:
        case LandUseCategory.BUILDINGS:
            return 'geometry:polygon and building=* and not building=no'
        case _:
            raise ValueError(f'{category} does not have an osm filter')


def calculate_area(gdf_input: gpd.GeoSeries) -> float:
    if gdf_input.empty:
        return 0.0
    projected_input = gdf_input.to_crs(gdf_input.estimate_utm_crs())
    return projected_input.area.sum()
