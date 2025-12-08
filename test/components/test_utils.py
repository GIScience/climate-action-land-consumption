import geopandas as gpd
from shapely.geometry.polygon import Polygon

from land_consumption.components.utils import clip_to_aoi, clip_geometries
from land_consumption.components.landuse_category_mappings import LandObjectCategory


def test_clip_geometries_no_interior_intersection(categories_gdf):
    categories_gdf['category'] = categories_gdf['category'].map(lambda x: LandObjectCategory[x])
    result = clip_geometries(categories_gdf)

    assert result.crs.to_string() == 'EPSG:4326'

    # Check that interiors of geometries do not intersect
    for i, geom1 in enumerate(result['geometry']):
        for j, geom2 in enumerate(result['geometry']):
            if i != j:
                # DE-9IM relationship
                assert geom1.relate(geom2) in ('FF*FF****', 'F0FFFF212', 'FF2F11212')

    assert set(result['category']) == set(categories_gdf['category'])
    assert len(result) == len(categories_gdf)


def test_clip_to_aoi():
    aoi_geom = Polygon(
        [
            (16.369602655, 48.21069154),
            (16.403734349, 48.21069154),
            (16.403734349, 48.229465035),
            (16.369602655, 48.229465035),
            (16.369602655, 48.21069154),
        ]
    )
    polygon_gdf = gpd.read_file('resources/test/test_clip_to_aoi_data.gpkg')
    geom_type = "'Polygon', 'MultiPolygon'"

    polygon_gdf = clip_to_aoi(polygon_gdf=polygon_gdf, aoi_geom=aoi_geom, geom_type=geom_type)

    assert all(polygon_gdf.is_valid)
