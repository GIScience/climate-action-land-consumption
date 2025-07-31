The **Land Consumption** plugin currently has three outputs: a basic table of land consumption, a detailed table, and a treemap. In all three cases, the plugin uses OpenStreetMap data to identify how land is used and consumed in a given area.

For the basic table, Land Consumption plugin reports the total land area and proportion of total land area for each land use object (e.g., buildings, roads, parking lots) as well as the proportion each object contributes to "consumed land" and "settled land" in the given area. The detailed table goes one step further and calculates how much land is consumed and used for each land use object and land use class (e.g., commercial, residential, industrial). Finally, the treemap reports the same information as the detailed table but as a hierarchical visual to show which land use objects and classes consume the most land in a given area.

### Definitions

**Consumed Land**
- Developed land including buildings, roads, parking areas, and other built infrastructure, but does not include agricultural, natural or semi-natural areas.

**Settled Land**
- Includes all land that is human-altered or developed such as agriculture, buildings, and paved areas, but excludes natural land.

### Land Use Object and Class Identification with OSM

The Land Consumption plugin currently considers the following land use objects when estimating land consumption in a given area. Some of these objects such as Agricultural Land are not objects themselves, but are included in the object category in order to understand how land is being used and consumed.

1. **Buildings**: Tagged as `building=*` in OpenStreetMap (OSM).
2. **Parking Lots**: Identified using the tags `amenity=parking` and `parking=surface`. Note: This excludes multi-level parking structures and on-street parking.
3. **Roads**: Tagged as `highway=*`. Road area is calculated using the width tag. If the width is not available,f the most common width for the respective road type in the area (based on its highway tag) is used.
4. **Built-up Land**: Refers to areas immediately surrounding OSM objects with `landuse` tags such as `residential`, `commercial`, or `industrial`. These are not discrete objects like buildings or roads, but include surfaces such as paved areas, courtyards, and plazas.
5. **Agricultural Land**: Includes land tagged as `landuse` with values including: `allotment`, `farmland`, `farmyard`, `meadow`, `orchard`, `plant_nursery`, `vineyard`. Agricultural land is excluded from consumed land calculations.
6. **Natural land**: Represents undeveloped or natural areas tagged as `natural=*`, `leisure=nature_reserve` or `landuse` with values including: `beach` or `forest`. Natural land is excluded from both consumed and settled land calculations.
7. **Other**: A catch-all category for land use types not covered by the above classes. Other is excluded from consumed land calculations.

In addition to land use objects, the Land Consumption plugin also considers the following land use classes in its calculations:

1. **Residential**: Includes land primarily used for housing and dwellings. OSM tag: `landuse=residential`
2. **Commercial**: Includes land used for business, retail, and service-related functions. OSM tags: `landuse=commercial` or `landuse=retail`
3. **Industrial**: Includes land designated for manufacturing, warehousing, or other industrial activities. OSM tag: `landuse=industrial`
4. **Infrastructural and Institutional Areas**: Covers land used for public infrastructure and institutional functions such as construction sites, religious facilities, and transport. OSM tags:  'garages', 'railway', 'harbour', 'port', 'lock', 'marina', 'construction', 'brownfield', 'military'

5. **Agricultural**: Represents land used for farming, horticulture, and related rural activities. OSM landuse values: `allotments`, `farmland`, `farmyard`, `meadow`, `orchard`, `plant_nursery`, `vineyard`
6. **Natural**: Represents undeveloped or natural areas. OSM tags: `natural=*`, `leisure=nature_reserve` or `landuse` with values including: `beach` or `forest`.
7. **Other Land Uses**: A catch-all category for miscellaneous land use classes that do not fit into the categories above.


### Land Consumption Output Variables

The basic and detailed tables as well as the treemap include the following three output variables:

**Total Land Area [ha]**
- The total land area, in hectares, occupied by a given land use object.
- Example: Buildings occupy 31.22 ha in Bergheim, Heidelberg.

**% of Consumed Land Area**
- The proportion of consumed land taken up by each land use object.
- Consumed land includes only explicitly developed land, such as buildings, roads, and parking lots.
- Example: Buildings account for 35.38% of consumed land in Bergheim, Heidelberg.

**% of Settled Land Area**
- The proportion of settled land occupied by each land use object.
- Settled land includes both consumed land and agricultural land, but excludes natural land.
- Example: Buildings account for 22.94% of settled land in Bergheim, Heidelberg.

### Data
The Land Consumption plugin is based on [OSM](https://www.openstreetmap.org/about) data.
OSM is a free and open geo-database with rich information about streets, paths,
and other important walkable infrastructure. OSM is created and maintained by volunteers. If the data for your area
of interest seem inaccurate and/or incomplete, you can help improve them by mapping your area in OSM using,
for example, the [StreetComplete](https://streetcomplete.app/) app (currently only available for Android).
