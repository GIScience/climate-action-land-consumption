import logging

import geopandas as gpd
import pandas as pd
import shapely
from _duckdb import DuckDBPyConnection
from ohsome import OhsomeClient
from pyiceberg.catalog.rest import RestCatalog

from land_consumption.components.landuse_category_mappings import (
    LandObjectCategory,
    LandUseCategory,
    LANDUSE_VALUE_MAP,
    AMENITY_INSTITUTIONAL_TAGS,
    AMENITY_INFRASTRUCTURE_TAGS,
    GEOM_TYPE_LOOKUP,
    NATURAL_EXCLUDE_VALUES,
)
from land_consumption.components.osm_requests import (
    get_osm_data_from_parquet,
    get_osm_data_from_parquet_duckdb,
    get_osm_data_from_ohsomepy,
)
from land_consumption.components.utils import clip_to_aoi, clip_geometries
from land_consumption.components.process_geometries import PROCESSING_REGISTRY

log = logging.getLogger(__name__)


def get_categories_gdf(
    aoi_geom: shapely.Polygon | shapely.MultiPolygon,
    data_connection: RestCatalog | tuple[RestCatalog, DuckDBPyConnection] | OhsomeClient,
) -> gpd.GeoDataFrame:
    features = request_osm_features(aoi_geom, data_connection)
    return clean_overlapping_features(features['land_objects'], features['land_use'])


def request_osm_features(
    aoi_geom: shapely.Polygon | shapely.MultiPolygon,
    data_connection: RestCatalog | tuple[RestCatalog, DuckDBPyConnection] | OhsomeClient,
) -> dict[str, gpd.GeoDataFrame]:
    xmin, ymin, xmax, ymax = aoi_geom.bounds
    categories_gdf = gpd.GeoDataFrame()

    landuse_polygons = []
    for geom_type, categories in GEOM_TYPE_LOOKUP.items():
        status = 'latest'

        row_filter = (
            f"status = '{status}' "
            f'and geometry_type IN ({geom_type}) '
            f'and (xmax >= {xmin} and xmin <= {xmax}) '
            f'and (ymax >= {ymin} and ymin <= {ymax}) '
        )

        selected_fields = ('tags', 'geometry')

        if isinstance(data_connection, RestCatalog):
            log.info('Getting osm data from parquet')
            polygon_gdf = get_osm_data_from_parquet(
                row_filter=row_filter, selected_fields=selected_fields, catalog=data_connection
            )
        elif isinstance(data_connection, tuple):
            log.info('Getting osm data from parquet with DuckDB')
            polygon_gdf = get_osm_data_from_parquet_duckdb(
                aoi_geom=aoi_geom,
                row_filter=row_filter,
                selected_fields=selected_fields,
                catalog=data_connection[0],
                con=data_connection[1],
            )
        elif isinstance(data_connection, OhsomeClient):
            log.info('Getting osm data from ohsome-py')
            polygon_gdf = get_osm_data_from_ohsomepy(
                aoi_geom=aoi_geom, geom_type=geom_type, selected_fields=selected_fields, client=data_connection
            )
        else:
            raise NotImplementedError(f'Data connection type not implemented: {data_connection}')

        polygon_gdf = clip_to_aoi(polygon_gdf=polygon_gdf, aoi_geom=aoi_geom, geom_type=geom_type)

        for category in categories:
            log.info(f'Processing category {category}')
            category_gdf = polygon_gdf[polygon_gdf['tags'].apply(get_land_object_filter(category))]

            try:
                category_gdf = category_gdf.set_crs('EPSG:4326')
                # fails as AttributeError if there's no active geometry column e.g. in an empty GeoDataFrame

            except AttributeError:
                log.warning(f'Received empty dataframe for {category}. Continuing with empty GeoDataFrame')
                category_gdf = gpd.GeoDataFrame(columns=polygon_gdf.columns)

            processing_function = PROCESSING_REGISTRY.get(category)
            category_gdf = processing_function(category_gdf)
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
    man_made = tags.get('man_made')

    if landuse_category := LANDUSE_VALUE_MAP.get(landuse):
        return landuse_category
    elif leisure == 'nature_reserve':
        return LandUseCategory.NATURAL
    elif amenity in AMENITY_INSTITUTIONAL_TAGS:
        return LandUseCategory.INSTITUTIONAL
    elif amenity in AMENITY_INFRASTRUCTURE_TAGS:
        return LandUseCategory.INFRASTRUCTURE
    elif natural is not None and natural not in NATURAL_EXCLUDE_VALUES:
        return LandUseCategory.NATURAL
    elif natural in NATURAL_EXCLUDE_VALUES:
        return LandUseCategory.UNKNOWN
    elif all(tag is None for tag in (landuse, natural, leisure, amenity, man_made)):
        return LandUseCategory.UNKNOWN
    else:
        return LandUseCategory.OTHER


def clean_overlapping_features(categories_gdf: gpd.GeoDataFrame, landuses_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
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
            if landuse_geom.geom_type == 'GeometryCollection':
                landuse_geom = gpd.GeoSeries(data=landuse_geom.geoms)
                landuse_geom = landuse_geom[landuse_geom.geom_type.isin(['Polygon', 'MultiPolygon'])].union_all()
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
        & (landobjects_with_landuse['landuse_category'] == LandUseCategory.OTHER),
        'category',
    ] = LandObjectCategory.OTHER

    landobjects_with_landuse.loc[
        landobjects_with_landuse['landuse_category'] == LandUseCategory.UNKNOWN,
        'category',
    ] = LandObjectCategory.UNKNOWN

    unknown = landobjects_with_landuse[landobjects_with_landuse['category'] == LandObjectCategory.UNKNOWN]
    known = landobjects_with_landuse[landobjects_with_landuse['category'] != LandObjectCategory.UNKNOWN]

    unknown_dissolved = unknown.dissolve()

    landobjects_unknown_dissolved = pd.concat([known, unknown_dissolved], ignore_index=True)

    return landobjects_unknown_dissolved
