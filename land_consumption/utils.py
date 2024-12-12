import logging
from enum import Enum
from typing import Tuple, Callable

import geopandas as gpd
import pandas as pd
import shapely
from geopandas import GeoDataFrame
from pandas import DataFrame
from pyiceberg.catalog.rest import RestCatalog
from tqdm import tqdm

log = logging.getLogger(__name__)

SQM_TO_HA_FACTOR = 1.0 / (100.0 * 100.0)


# Order in which the categories are clipped
class LandUseCategory(Enum):
    BUILDINGS = 'Buildings'
    PARKING_LOTS = 'Parking lots'
    PAVED_ROADS = 'Paved Roads'
    UNPAVED_ROADS = 'Unpaved Roads'
    BUILT_UP = 'Other built up area'
    UNKNOWN = 'Unknown consumption'


# arbitrary, could e user input or flexible
CONSUMPTION_FACTOR_LOOKUP = {
    LandUseCategory.BUILDINGS: 1.0,
    LandUseCategory.PARKING_LOTS: 0.8,
    LandUseCategory.BUILT_UP: 0.75,
    LandUseCategory.PAVED_ROADS: 1,
    LandUseCategory.UNPAVED_ROADS: 0.2,
    LandUseCategory.UNKNOWN: None,
}

GEOM_TYPE_LOOKUP = {
    "'Polygon', 'MultiPolygon'": [LandUseCategory.BUILDINGS, LandUseCategory.PARKING_LOTS, LandUseCategory.BUILT_UP],
    "'LineString', 'MultiLineString'": [LandUseCategory.PAVED_ROADS, LandUseCategory.UNPAVED_ROADS],
}


def get_filter_functions(category: LandUseCategory) -> callable:
    match category:
        case LandUseCategory.BUILDINGS:
            return lambda x: 'building' in x.keys() and x['building'] != 'no'
        case LandUseCategory.PARKING_LOTS:
            return (
                lambda x: 'amenity' in x.keys()
                and x['amenity'] == 'parking'
                and 'parking' in x.keys()
                and x['parking'] == 'surface'
            )
        case LandUseCategory.PAVED_ROADS:
            return lambda x: 'highway' in x.keys() and (
                (
                    'surface' in x.keys()
                    and x['surface']
                    in (
                        'paved',
                        'asphalt',
                        'chipseal',
                        'concrete',
                        'concrete:lanes',
                        'concrete:plates',
                        'paving_stones',
                        'paving_stones:lanes',
                        'grass_paver',
                        'sett',
                        'unhewn_cobblestone',
                        'cobblestone',
                        'cobblestone:flattened',
                        'bricks',
                        'metal',
                        'metal_grid',
                        'wood',
                        'stepping_stones',
                        'rubber',
                        'tiles',
                    )
                )
                or 'surface' not in x.keys()
            )
        case LandUseCategory.UNPAVED_ROADS:
            return (
                lambda x: 'highway' in x.keys()
                and 'surface' in x.keys()
                and x['surface']
                in (
                    'unpaved',
                    'compacted',
                    'fine_gravel',
                    'gravel',
                    'shells',
                    'rock',
                    'pebblestone',
                    'ground',
                    'dirt',
                    'earth',
                    'grass',
                    'mud',
                    'sand',
                    'woodchips',
                    'snow',
                    'ice',
                    'salt',
                    'user_defined',
                )
            )

        case LandUseCategory.BUILT_UP:
            return lambda x: 'landuse' in x.keys() and (
                x['landuse']
                in (
                    'residential',
                    'garages',
                    'railway',
                    'industrial',
                    'commercial',
                    'retail',
                    'harbour',
                    'port',
                    'lock',
                    'marina',
                )
                or ('amenity' in x.keys() and x['amenity'] in ('university', 'school'))
            )
        case _:
            raise ValueError(f'{category} does not have a filter function')


def get_osm_data_from_parquet(
    aoi_geom: shapely.MultiPolygon | shapely.Polygon,
    row_filter: str,
    selected_fields: Tuple[str, str],
    catalog: RestCatalog,
) -> GeoDataFrame:
    namespace = 'geo_sort'
    tablename = 'contributions'

    icebergtable = catalog.load_table((namespace, tablename))
    table = icebergtable.scan(
        row_filter=row_filter,
        selected_fields=selected_fields,
    )
    df = table.to_pandas()
    log.debug(f'Retrieved {df.shape[0]} rows from iceberg')

    df['tags'] = df['tags'].apply(lambda x: dict(x))

    gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['geometry']))

    gdf = gdf.clip(aoi_geom)

    return gdf


def get_mode_values_for_road_types(highways_with_width: DataFrame) -> DataFrame:
    # our assumption values of road width for a specific road type
    highways_with_width = highways_with_width.dropna(subset=['width'])
    width_per_type = highways_with_width.groupby(['type'])['width'].agg(lambda x: pd.Series.mode(x).max()).to_dict()
    return width_per_type


def assign_road_width(highway_df: GeoDataFrame) -> GeoDataFrame:
    highway_df.loc[:, 'width'] = highway_df['tags'].apply(get_width_value)
    highway_df['width'] = pd.to_numeric(highway_df['width'], errors='coerce')

    highway_df.loc[:, 'type'] = highway_df['tags'].apply(get_road_type)

    highway_df.loc[:, 'lanes'] = highway_df['tags'].apply(get_number_of_lanes)

    width_per_type = get_mode_values_for_road_types(highway_df)

    mask = highway_df['width'].isna()

    # Update 'width' only where the mask is True, with default value 3
    highway_df.loc[mask, 'width'] = highway_df.loc[mask, 'type'].apply(lambda x: width_per_type.get(x, 3))

    return highway_df


def get_width_value(tags: dict) -> float | None:
    return tags.get('width', None)


def get_road_type(tags: dict) -> str | None:
    return tags.get('highway', None)


# Function to safely extract highway number of lanes
def get_number_of_lanes(tags: dict) -> int:
    # Assumption: If no lane info then its single lane road
    return tags.get('lanes', 1)


def generate_buffer(highway_df: DataFrame) -> GeoDataFrame:
    # Create a new GeoDataFrame with buffered geometries
    highway_gdf = gpd.GeoDataFrame(highway_df, geometry='geometry').set_crs('epsg:4326')

    # Check for and fix invalid geometries
    highway_gdf['geometry'] = highway_gdf['geometry'].apply(
        lambda geom: geom if geom.is_valid else geom.buffer(0)  # Buffer(0) can fix minor issues
    )

    # estimate UTM projection in meters for the data we have
    all_buffered_highways = highway_gdf.to_crs(highway_gdf.estimate_utm_crs())

    # Buffer in the projected CRS using width_value
    all_buffered_highways['geometry'] = all_buffered_highways.apply(
        lambda row: row['geometry'].buffer(row['width'] / 2, resolution=4, cap_style='flat'),
        axis=1,
    )

    # Reproject the buffered geometries back to EPSG:4326
    all_buffered_highways_gdf = all_buffered_highways.to_crs(4326)

    # Set the final GeoDataFrame geometry
    all_buffered_highways_gdf = gpd.GeoDataFrame(all_buffered_highways_gdf, geometry='geometry', crs='EPSG:4326')

    return get_union(all_buffered_highways_gdf)


def calculate_area(gdf: GeoDataFrame) -> GeoDataFrame:
    gdf.index = range(len(gdf))
    if gdf.empty:
        gdf['area'] = 0.0
        return gdf

    gdf['area'] = 0.0

    projected_gdf = gdf.to_crs(gdf.estimate_utm_crs())
    for i, geom in projected_gdf.iterrows():
        if geom.geometry is None:
            gdf.at[i, 'area'] = 0
        else:
            gdf.at[i, 'area'] = geom.geometry.area

    return gdf


def get_union(category_gdf: GeoDataFrame) -> GeoDataFrame:
    if category_gdf.empty:
        return category_gdf
    category_gdf = GeoDataFrame(geometry=[category_gdf.unary_union])
    return category_gdf


def clip_geometries(categories_gdf: GeoDataFrame) -> GeoDataFrame:
    clipped_gdf = gpd.GeoDataFrame()
    for category in LandUseCategory:
        if category != LandUseCategory.UNKNOWN:
            category_gdf = categories_gdf[categories_gdf['category'] == category.name]
            if not category_gdf['geometry'].isnull().all():
                if not clipped_gdf.empty:
                    category_gdf['geometry'] = category_gdf.geometry.difference(clipped_gdf.unary_union)
            clipped_gdf = pd.concat([clipped_gdf, category_gdf])
    clipped_gdf = clipped_gdf.set_crs(4326)
    return clipped_gdf


def get_categories_gdf(aoi_geom: shapely.Polygon | shapely.MultiPolygon, catalog: RestCatalog):
    xmin, ymin, xmax, ymax = aoi_geom.bounds
    categories_gdf = gpd.GeoDataFrame()

    def get_processing_function(category: LandUseCategory) -> Callable[[GeoDataFrame], GeoDataFrame]:
        if category in {LandUseCategory.BUILDINGS, LandUseCategory.PARKING_LOTS, LandUseCategory.BUILT_UP}:
            return get_union
        elif category in {LandUseCategory.PAVED_ROADS, LandUseCategory.UNPAVED_ROADS}:
            return process_roads
        else:
            raise ValueError(f'{category} does not have a processing function')

    def process_roads(category_gdf: GeoDataFrame) -> GeoDataFrame:
        if category_gdf.empty:
            return get_union(category_gdf)
        roads_with_width = assign_road_width(category_gdf)

        return generate_buffer(roads_with_width)

    for geom_type, categories in tqdm(GEOM_TYPE_LOOKUP.items()):
        status = 'latest'

        row_filter = (
            f"status = '{status}' "
            f'and geometry_type IN ({geom_type}) '
            f'and (bbox.xmax >= {xmin} and bbox.xmin <= {xmax}) '
            f'and (bbox.ymax >= {ymin} and bbox.ymin <= {ymax}) '
        )

        selected_fields = ('tags', 'geometry')

        polygon_gdf = get_osm_data_from_parquet(aoi_geom, row_filter, selected_fields, catalog=catalog)
        for category in tqdm(categories, leave=False):
            log.info(f'Processing category {category}')
            category_gdf = polygon_gdf[polygon_gdf['tags'].apply(get_filter_functions(category))]
            if category_gdf.empty:
                category_gdf = gpd.GeoDataFrame(columns=polygon_gdf.columns)
            category_gdf = get_processing_function(category)(category_gdf)
            category_gdf['category'] = [category.name]
            categories_gdf = pd.concat([categories_gdf, category_gdf])

    categories_gdf = clip_geometries(categories_gdf)

    return categories_gdf
