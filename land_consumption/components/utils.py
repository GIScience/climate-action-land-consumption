import geopandas as gpd
import pandas as pd
import shapely
from geopandas import GeoDataFrame

from land_consumption.components.landuse_category_mappings import LandObjectCategory


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


def clip_to_aoi(
    polygon_gdf: gpd.GeoDataFrame,
    aoi_geom: shapely.Polygon | shapely.MultiPolygon,
    geom_type: str,
) -> gpd.GeoDataFrame:
    if all(polygon_gdf.is_valid):
        polygon_gdf = polygon_gdf.clip(aoi_geom)
    else:
        # make_valid doesn't work with mixed-dimension geometries, so explode first
        polygon_gdf = polygon_gdf.explode(ignore_index=True)
        polygon_gdf['geometry'] = polygon_gdf['geometry'].make_valid()
        polygon_gdf = polygon_gdf.explode(ignore_index=True)

        target_geoms = [t.strip('\'"') for t in geom_type.split(', ')]
        polygon_gdf = polygon_gdf[polygon_gdf['geometry'].geom_type.isin(target_geoms)]

        polygon_gdf = polygon_gdf.clip(aoi_geom)

    return polygon_gdf


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
