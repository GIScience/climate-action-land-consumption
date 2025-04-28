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
class LandObjectCategory(Enum):
    BUILDINGS = 'Buildings'
    PARKING_LOTS = 'Parking lots'
    ROADS = 'Roads'
    BUILT_UP = 'Built up land'
    OTHER = 'Other'


class LandUseCategory(Enum):
    BUILT_UP = 'Other built up area'
    COMMERCIAL = 'Commercial built up area'
    RESIDENTIAL = 'Residential built up area'
    INDUSTRIAL = 'Industrial built up area'
    AGRICULTURAL = 'Agricultural areas'
    OTHER = 'Other land uses'


GEOM_TYPE_LOOKUP = {
    "'Polygon', 'MultiPolygon'": [LandObjectCategory.BUILDINGS, LandObjectCategory.PARKING_LOTS],
    "'LineString', 'MultiLineString'": [LandObjectCategory.ROADS],
}


def get_land_object_filter(category: LandObjectCategory) -> callable:
    match category:
        case LandObjectCategory.BUILDINGS:
            return lambda x: 'building' in x.keys() and x['building'] != 'no'
        case LandObjectCategory.PARKING_LOTS:
            return (
                lambda x: 'amenity' in x.keys()
                and x['amenity'] == 'parking'
                and 'parking' in x.keys()
                and x['parking'] == 'surface'
            )
        case LandObjectCategory.ROADS:
            return lambda x: 'highway' in x.keys()
        case _:
            raise ValueError(f'{category} does not have a filter function')


def get_land_use_filter(tags: dict) -> LandUseCategory | None:
    landuse = tags.get('landuse')
    match landuse:
        case 'garages' | 'railway' | 'harbour' | 'port' | 'lock' | 'marina':
            return LandUseCategory.BUILT_UP
        case 'commercial' | 'retail':
            return LandUseCategory.COMMERCIAL
        case 'residential':
            return LandUseCategory.RESIDENTIAL
        case 'industrial':
            return LandUseCategory.INDUSTRIAL
        case 'allotments' | 'farmland' | 'farmyard' | 'meadow' | 'orchard' | 'orchard' | 'plant_nursery' | 'vineyard':
            return LandUseCategory.AGRICULTURAL
        case _:
            return LandUseCategory.OTHER


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

    highway_df.loc[mask, 'width'] = highway_df.loc[mask, 'type'].apply(lambda x: width_per_type.get(x, 3))

    return highway_df


def get_width_value(tags: dict) -> float | None:
    return tags.get('width', None)


def get_road_type(tags: dict) -> str | None:
    return tags.get('highway', None)


def get_number_of_lanes(tags: dict) -> int:
    return tags.get('lanes', 1)


def generate_buffer(highway_df: DataFrame) -> GeoDataFrame:
    highway_gdf = gpd.GeoDataFrame(highway_df, geometry='geometry').set_crs('epsg:4326')

    highway_gdf['geometry'] = highway_gdf['geometry'].apply(
        lambda geom: geom if geom.is_valid else geom.buffer(0)  # Buffer(0) can fix minor issues
    )

    all_buffered_highways = highway_gdf.to_crs(highway_gdf.estimate_utm_crs())

    all_buffered_highways['geometry'] = all_buffered_highways.apply(
        lambda row: row['geometry'].buffer(row['width'] / 2, resolution=4, cap_style='flat'),
        axis=1,
    )

    all_buffered_highways_gdf = all_buffered_highways.to_crs(4326)

    all_buffered_highways_gdf = gpd.GeoDataFrame(all_buffered_highways_gdf, geometry='geometry', crs='EPSG:4326')

    return get_union(all_buffered_highways_gdf)


def calculate_area(gdf: GeoDataFrame) -> GeoDataFrame:
    gdf.reset_index(inplace=True)

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
    category_gdf = GeoDataFrame(geometry=[category_gdf.union_all()])
    return category_gdf


def clip_geometries(categories_gdf: GeoDataFrame) -> GeoDataFrame:
    clipped_gdf = gpd.GeoDataFrame()
    for category in LandObjectCategory:
        if category != LandObjectCategory.OTHER:
            category_gdf = categories_gdf[categories_gdf['category'] == category.name]
            if not category_gdf['geometry'].isnull().all():
                if not clipped_gdf.empty:
                    category_gdf['geometry'] = category_gdf.geometry.difference(clipped_gdf.union_all())
            clipped_gdf = pd.concat([clipped_gdf, category_gdf])
    clipped_gdf = clipped_gdf.set_crs(4326)
    return clipped_gdf


def get_categories_gdf(aoi_geom: shapely.Polygon | shapely.MultiPolygon, catalog: RestCatalog):
    xmin, ymin, xmax, ymax = aoi_geom.bounds
    categories_gdf = gpd.GeoDataFrame()

    def get_processing_function(category: LandObjectCategory) -> Callable[[GeoDataFrame], GeoDataFrame]:
        if category in {LandObjectCategory.BUILDINGS, LandObjectCategory.PARKING_LOTS}:
            return get_union
        elif category in {LandObjectCategory.ROADS}:
            return process_roads
        else:
            raise ValueError(f'{category} does not have a processing function')

    def process_roads(category_gdf: GeoDataFrame) -> GeoDataFrame:
        if category_gdf.empty:
            return get_union(category_gdf)
        roads_with_width = assign_road_width(category_gdf)

        return generate_buffer(roads_with_width)

    landuse_polygons = []
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
            category_gdf = polygon_gdf[polygon_gdf['tags'].apply(get_land_object_filter(category))]
            if category_gdf.empty:
                category_gdf = gpd.GeoDataFrame(columns=polygon_gdf.columns)
            category_gdf = get_processing_function(category)(category_gdf)
            category_gdf['category'] = [category.name]
            categories_gdf = pd.concat([categories_gdf, category_gdf])

        polygon_gdf['landuse_category'] = polygon_gdf.tags.apply(get_land_use_filter)
        for category in LandUseCategory:
            landuse_gdf = polygon_gdf[polygon_gdf['landuse_category'] == category]
            landuse_polygons.append(landuse_gdf)

    categories_gdf = clip_geometries(categories_gdf)
    landuses_gdf = pd.concat(landuse_polygons)
    landuses_gdf = landuses_gdf[landuses_gdf['geometry'].geom_type.isin(['MultiPolygon', 'Polygon'])]
    landuses_gdf = landuses_gdf.dissolve(by='landuse_category', sort=False)

    category_by_landuse = []
    non_matching_landuse = []
    for category in landuses_gdf.index:
        landuse_geom = landuses_gdf.at[category, 'geometry']
        clipped_categories_gdf = categories_gdf.clip(landuse_geom)
        clipped_categories_gdf['landuse_category'] = category.name
        category_by_landuse.append(clipped_categories_gdf)
        remaining_landuse = landuse_geom.difference(categories_gdf.union_all())
        remaining_gdf = gpd.GeoDataFrame(
            {'geometry': [remaining_landuse], 'category': 'BUILT_UP', 'landuse_category': [category.name]}
        )
        non_matching_landuse.append(remaining_gdf)

    landobjects_with_landuse = pd.concat(category_by_landuse + non_matching_landuse)
    landobjects_with_landuse.loc[
        (landobjects_with_landuse['category'] == 'BUILT_UP')
        & (landobjects_with_landuse['landuse_category'] == 'OTHER'),
        'category',
    ] = 'OTHER'

    return landobjects_with_landuse


def custom_land_use_sort(row):
    if row['Land Use Class'] == 'Subtotal':
        return 2
    elif row['Land Use Class'] == 'Other land uses':
        return 1
    else:
        return 0
