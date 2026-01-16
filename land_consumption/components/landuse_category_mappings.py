from enum import Enum


class LandObjectCategory(Enum):
    BUILDINGS = 'Buildings'
    PARKING_LOTS = 'Parking lots'
    ROADS = 'Roads'
    BUILT_UP = 'Built up land'
    OTHER = 'Other'
    UNKNOWN = 'Unknown'


class LandUseCategory(Enum):
    COMMERCIAL = 'Commercial'
    RESIDENTIAL = 'Residential'
    INDUSTRIAL = 'Industrial'
    INFRASTRUCTURE = 'Infrastructure'
    INSTITUTIONAL = 'Institutional'
    AGRICULTURAL = 'Agricultural'
    NATURAL = 'Natural'
    OTHER = 'Other land uses'
    UNKNOWN = 'Unknown land uses'


LANDUSE_VALUE_MAP = {
    'garages': LandUseCategory.INFRASTRUCTURE,
    'railway': LandUseCategory.INFRASTRUCTURE,
    'harbour': LandUseCategory.INFRASTRUCTURE,
    'port': LandUseCategory.INFRASTRUCTURE,
    'lock': LandUseCategory.INFRASTRUCTURE,
    'marina': LandUseCategory.INFRASTRUCTURE,
    'military': LandUseCategory.INSTITUTIONAL,
    'religious': LandUseCategory.INSTITUTIONAL,
    'cemetery': LandUseCategory.INSTITUTIONAL,
    'commercial': LandUseCategory.COMMERCIAL,
    'retail': LandUseCategory.COMMERCIAL,
    'residential': LandUseCategory.RESIDENTIAL,
    'industrial': LandUseCategory.INDUSTRIAL,
    'allotments': LandUseCategory.AGRICULTURAL,
    'farmland': LandUseCategory.AGRICULTURAL,
    'farmyard': LandUseCategory.AGRICULTURAL,
    'meadow': LandUseCategory.AGRICULTURAL,
    'orchard': LandUseCategory.AGRICULTURAL,
    'plant_nursery': LandUseCategory.AGRICULTURAL,
    'vineyard': LandUseCategory.AGRICULTURAL,
    'forest': LandUseCategory.NATURAL,
}
AMENITY_INSTITUTIONAL_TAGS = [
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
]
AMENITY_INFRASTRUCTURE_TAGS = [
    'bus_station',
    'ferry_terminal',
]
NATURAL_EXCLUDE_VALUES = [
    'valley',
    'hill',
    'ridge',
    'cape',
    'peninsula',
    'cliff',
]
GEOM_TYPE_LOOKUP = {
    "'LineString', 'MultiLineString'": [LandObjectCategory.ROADS],
    "'Polygon', 'MultiPolygon'": [LandObjectCategory.BUILDINGS, LandObjectCategory.PARKING_LOTS],
}
