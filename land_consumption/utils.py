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


class LandObjectCategory(Enum):
    BUILDINGS = 'Buildings'
    PARKING_LOTS = 'Parking lots'
    ROADS = 'Roads'
    BUILT_UP = 'Built up land'
    OTHER = 'Other'


class LandUseCategory(Enum):
    COMMERCIAL = 'Commercial'
    RESIDENTIAL = 'Residential'
    INDUSTRIAL = 'Industrial'
    INFRASTRUCTURE = 'Infrastructure'
    INSTITUTIONAL = 'Institutional'
    AGRICULTURAL = 'Agricultural'
    NATURAL = 'Natural'
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
    natural = tags.get('natural')
    leisure = tags.get('leisure')
    amenity = tags.get('amenity')

    match landuse:
        case 'garages' | 'railway' | 'harbour' | 'port' | 'lock' | 'marina':
            return LandUseCategory.INFRASTRUCTURE
        case 'military' | 'religious' | 'cemetery':
            return LandUseCategory.INSTITUTIONAL
        case 'commercial' | 'retail':
            return LandUseCategory.COMMERCIAL
        case 'residential':
            return LandUseCategory.RESIDENTIAL
        case 'industrial':
            return LandUseCategory.INDUSTRIAL
        case 'allotments' | 'farmland' | 'farmyard' | 'meadow' | 'orchard' | 'orchard' | 'plant_nursery' | 'vineyard':
            return LandUseCategory.AGRICULTURAL
        case 'beach' | 'forest':
            return LandUseCategory.NATURAL
    if leisure == 'nature_reserve':
        return LandUseCategory.NATURAL
    if amenity in [
        'university',
        'school',
        'college',
        'hospital',
        'clinic',
        'community_centre',
        'courthouse',
        'fire_station',
        'police_station',
        'prison',
        'townhall',
        'monastery',
        'place_of_worship',
    ]:
        return LandUseCategory.INSTITUTIONAL
    if amenity in ['bus_station', 'ferry_terminal', 'college', 'hospital', 'clinic']:
        return LandUseCategory.INFRASTRUCTURE
    if natural is not None:
        return LandUseCategory.NATURAL

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

    all_buffered_highways = all_buffered_highways.dissolve('width', as_index=False)

    all_buffered_highways['geometry'] = all_buffered_highways.apply(
        lambda row: row['geometry'].buffer(row['width'] / 2, resolution=2, cap_style='flat'),
        axis=1,
    )

    all_buffered_highways_gdf = all_buffered_highways.to_crs(4326)

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
            category_gdf = categories_gdf[categories_gdf['category'] == category]
            if not category_gdf['geometry'].isnull().all():
                if not clipped_gdf.empty:
                    category_gdf['geometry'] = category_gdf.geometry.difference(clipped_gdf.union_all())
            clipped_gdf = pd.concat([clipped_gdf, category_gdf])
    clipped_gdf = clipped_gdf.set_crs(4326)
    return clipped_gdf


def request_osm_features(
    aoi_geom: shapely.Polygon | shapely.MultiPolygon, catalog: RestCatalog
) -> dict[str, gpd.GeoDataFrame]:
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
            category_gdf['category'] = [category]
            categories_gdf = pd.concat([categories_gdf, category_gdf])

        polygon_gdf['landuse_category'] = polygon_gdf.tags.apply(get_land_use_filter)
        for category in LandUseCategory:
            landuse_gdf = polygon_gdf[polygon_gdf['landuse_category'] == category]
            landuse_polygons.append(landuse_gdf)

    return {
        'land_objects': categories_gdf,
        'land_use': gpd.GeoDataFrame(pd.concat(landuse_polygons, ignore_index=True)),
    }


def clean_overlapping_features(categories_gdf: gpd.GeoDataFrame, landuses_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    category_priority = list(LandObjectCategory)
    landuse_priority = list(LandUseCategory)

    categories_gdf = clip_geometries(categories_gdf)
    landuses_gdf = landuses_gdf.explode(ignore_index=True)
    landuses_gdf = landuses_gdf[landuses_gdf['geometry'].geom_type.isin(['Polygon', 'MultiPolygon'])]

    landuses_gdf = landuses_gdf.dissolve(by='landuse_category', sort=False)
    landuses_gdf['priority'] = landuses_gdf.index.map(landuse_priority.index)
    landuses_gdf = landuses_gdf.sort_values('priority')

    category_by_landuse = []
    non_matching_landuse = []
    used_geom = None

    for landuse_category, row in landuses_gdf.iterrows():
        landuse_geom = row.geometry

        if used_geom:
            landuse_geom = landuse_geom.difference(used_geom)
        if landuse_geom.is_empty:
            continue

        clipped_categories_gdf = categories_gdf.clip(landuse_geom)
        clipped_categories_gdf = clipped_categories_gdf.sort_values(
            'category', key=lambda x: x.map(lambda cat: category_priority.index(cat))
        )
        clipped_categories_gdf['landuse_category'] = landuse_category
        category_by_landuse.append(clipped_categories_gdf)

        remaining_landuse = landuse_geom.difference(clipped_categories_gdf.union_all())
        if not remaining_landuse.is_empty:
            remaining_gdf = gpd.GeoDataFrame(
                {
                    'geometry': [remaining_landuse],
                    'category': LandObjectCategory.BUILT_UP,
                    'landuse_category': landuse_category,
                },
                crs=categories_gdf.crs,
            )
            non_matching_landuse.append(remaining_gdf)

        if used_geom:
            used_geom = used_geom.union(landuse_geom)
        else:
            used_geom = landuse_geom

    landobjects_with_landuse = pd.concat(category_by_landuse + non_matching_landuse)

    landobjects_with_landuse.loc[
        (landobjects_with_landuse['category'] == LandObjectCategory.BUILT_UP)
        & (landobjects_with_landuse['landuse_category'] == LandObjectCategory.OTHER),
        'category',
    ] = LandObjectCategory.OTHER

    return landobjects_with_landuse


def get_categories_gdf(aoi_geom: shapely.Polygon | shapely.MultiPolygon, catalog: RestCatalog) -> gpd.GeoDataFrame:
    features = request_osm_features(aoi_geom, catalog)
    return clean_overlapping_features(features['land_objects'], features['land_use'])


def sort_land_consumption_table(
    df: pd.DataFrame,
    use_detailed_sort: bool = False,
) -> pd.DataFrame:
    category_order = [
        'Buildings',
        'Built up land',
        'Parking lots',
        'Roads',
        'Agricultural land',
        'Natural land',
        'Other',
        'Total',
    ]

    df['Land Use Object'] = pd.Categorical(df['Land Use Object'], categories=category_order, ordered=True)

    if use_detailed_sort:

        def custom_land_use_sort(row):
            if row['Land Use Class'] == 'Subtotal':
                return 2
            elif row['Land Use Class'] == 'Other land uses':
                return 1
            else:
                return 0

        df['__sort_order'] = df.apply(custom_land_use_sort, axis=1)
        df = df.sort_values(by=['Land Use Object', '__sort_order', 'Land Use Class'])
        df.drop(columns='__sort_order', inplace=True)
    else:
        df = df.sort_values('Land Use Object')

    df.set_index('Land Use Object', inplace=True)
    return df
