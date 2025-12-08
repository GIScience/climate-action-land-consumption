import geopandas as gpd
import pandas as pd
from geopandas import GeoDataFrame
from pandas import DataFrame

from land_consumption.components.landuse_category_mappings import LandObjectCategory


def get_union(category_gdf: GeoDataFrame) -> GeoDataFrame:
    if category_gdf.empty:
        return category_gdf
    category_gdf = GeoDataFrame(geometry=[category_gdf.union_all()])
    return category_gdf


def process_roads(category_gdf: GeoDataFrame) -> GeoDataFrame:
    if category_gdf.empty:
        return get_union(category_gdf)
    roads_with_width = assign_road_width(category_gdf)

    return generate_buffer(roads_with_width)


PROCESSING_REGISTRY = {
    LandObjectCategory.ROADS: process_roads,
    LandObjectCategory.BUILDINGS: get_union,
    LandObjectCategory.PARKING_LOTS: get_union,
}


def assign_road_width(highway_df: GeoDataFrame) -> GeoDataFrame:
    highway_df.loc[:, 'width'] = highway_df['tags'].apply(get_width_value)
    highway_df['width'] = pd.to_numeric(highway_df['width'], errors='coerce')

    highway_df.loc[:, 'type'] = highway_df['tags'].apply(get_road_type)

    highway_df.loc[:, 'lanes'] = highway_df['tags'].apply(get_number_of_lanes)

    width_per_type = get_mode_values_for_road_types(highway_df)

    mask = highway_df['width'].isna()

    highway_df.loc[mask, 'width'] = highway_df.loc[mask, 'type'].apply(lambda x: width_per_type.get(x, 3))

    return highway_df


def get_mode_values_for_road_types(highways_with_width: DataFrame) -> DataFrame:
    highways_with_width = highways_with_width.dropna(subset=['width'])
    width_per_type = highways_with_width.groupby(['type'])['width'].agg(lambda x: pd.Series.mode(x).max()).to_dict()
    return width_per_type


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
