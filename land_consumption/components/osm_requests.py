import logging
from typing import Tuple

import shapely
from climatoology.base.exception import ClimatoologyUserError
from geopandas import GeoDataFrame
from ohsome import OhsomeClient

from land_consumption.components.landuse_category_mappings import (
    AMENITY_INFRASTRUCTURE_TAGS,
    AMENITY_INSTITUTIONAL_TAGS,
    LANDUSE_VALUE_MAP,
)

log = logging.getLogger(__name__)


def get_osm_data(
    aoi_geom: shapely.MultiPolygon | shapely.Polygon,
    geom_type: str,
    selected_fields: Tuple[str, str],
    client: OhsomeClient,
) -> GeoDataFrame:
    row_filter = build_ohsome_filter(geom_type)

    check_path_count(aoi_geom=aoi_geom, client=client, count_limit=100000, row_filter=row_filter)

    ohsome_response = client.elements.geometry.post(
        properties='tags',
        bpolys=aoi_geom,
        filter=row_filter,
        clipGeometry=True,
    )
    gdf = ohsome_response.as_dataframe()
    gdf = gdf.reset_index(drop=True).rename(columns={'@other_tags': 'tags'})
    return gdf[list(selected_fields)]


def build_ohsome_filter(
    geom_type: str,
) -> str:
    if geom_type == "'LineString', 'MultiLineString'":
        return 'geometry:line and (highway=*)'

    elif geom_type == "'Polygon', 'MultiPolygon'":
        row_filter = []

        row_filter.append('(building=*)')
        row_filter.append('(amenity=parking and parking=surface)')

        landuse_values = ','.join(LANDUSE_VALUE_MAP.keys())
        row_filter.append(f'(landuse in ({landuse_values}))')

        row_filter.append('(natural=*)')
        row_filter.append('(leisure=nature_reserve)')
        row_filter.append('(man_made=*)')

        all_amenity_values = AMENITY_INSTITUTIONAL_TAGS + AMENITY_INFRASTRUCTURE_TAGS
        amenity_values = ','.join(all_amenity_values)
        row_filter.append(f'(amenity in ({amenity_values}))')

        return 'geometry:polygon and (' + ' or '.join(row_filter) + ')'

    raise ValueError('Unknown geometry-type!')


def check_path_count(
    aoi_geom: shapely.Polygon | shapely.MultiPolygon, client: OhsomeClient, count_limit: int, row_filter: str
) -> None:
    if row_filter.startswith('geometry:polygon'):
        return None
    ohsome_responses = client.elements.count.post(bpolys=aoi_geom, filter=row_filter).data
    path_lines_count = sum([response['value'] for response in ohsome_responses['result']])
    log.info(f'There are {path_lines_count} paths selected.')
    if path_lines_count > count_limit:
        raise ClimatoologyUserError(
            'There are too many OSM objects in the selected area. '
            'Please select a smaller area or a sub-region of your selected area.'
        )
