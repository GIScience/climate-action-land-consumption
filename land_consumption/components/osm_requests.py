from typing import Tuple

import geopandas as gpd
import logging
import shapely
from _duckdb import DuckDBPyConnection
from climatoology.base.exception import ClimatoologyUserError
from geopandas import GeoDataFrame
from ohsome import OhsomeClient
from pyiceberg.catalog.rest import RestCatalog

from land_consumption.components.landuse_category_mappings import (
    LANDUSE_VALUE_MAP,
    AMENITY_INSTITUTIONAL_TAGS,
    AMENITY_INFRASTRUCTURE_TAGS,
)

log = logging.getLogger(__name__)


def get_osm_data_from_parquet(
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
    return gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['geometry']))


def get_osm_data_from_parquet_duckdb(
    aoi_geom: shapely.MultiPolygon | shapely.Polygon,
    row_filter: str,
    selected_fields: Tuple[str, str],
    catalog: RestCatalog,
    con: DuckDBPyConnection,
) -> GeoDataFrame:
    namespace = 'geo_sort'
    tablename = 'contributions'
    icebergtable = catalog.load_table((namespace, tablename))
    scan = icebergtable.scan(row_filter=row_filter, selected_fields=selected_fields)
    files = [task.file.file_path for task in scan.plan_files()]
    log.debug('Parquet files that will be accessed:', files)

    # Get file sizes
    # settings = Settings()
    # client = Minio(
    #     endpoint=settings.ohsome_minio_endpoint,
    #     access_key=settings.ohsome_minio_access_key_id,
    #     secret_key=settings.ohsome_minio_access_key,
    #     region=settings.ohsome_minio_region,
    # )
    # sizes = [
    #     client.stat_object(
    #         bucket_name='heigit-ohsome-planet', object_name=file.replace('s3a://heigit-ohsome-planet/', '')
    #     ).size
    #     for file in files
    # ]
    # byte_to_gb_factor=0.00000000093132
    # print('Total size of files on the server:', sum(sizes) * byte_to_gb_factor, 'GB')

    sql = f"""
        SELECT
            osm_id,
            {','.join(selected_fields)}
        FROM read_parquet({files}) a
        WHERE 1=1
            and ST_Intersects(ST_GeomFromText(a.geometry), ST_GeomFromText('{aoi_geom.wkt}'))
    """
    df = con.sql(sql).df()

    gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkt(df['geometry']))
    gdf['tags'] = gdf['tags'].apply(lambda x: dict(x))
    return gdf


def get_osm_data_from_ohsomepy(
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
