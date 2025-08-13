The **Land Consumption** assesment tool currently has three outputs based on OpenStreetMap data, identifying how land is used and consumed in a given area.

Land Consumption reports how much land is consumed and used for each land use object (e.g., buildings, roads, parking lots) and land use class (e.g., commercial, residential, industrial) in a treemap and as a detailed table. The basic table provides a summary of this data.

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
4. **Infrastructure**: Covers land used for public infrastructure such as transport. OSM tags: 'railway', 'harbour', 'port', 'lock', 'marina'.
5. **Institutional**: Covers land used for public infrastructure and institutional functions such as universities, religious facilities. OSM tags:  'garages',  'military'.
6. **Agricultural**: Represents land used for farming, horticulture, and related rural activities. OSM landuse values: `allotments`, `farmland`, `farmyard`, `meadow`, `orchard`, `plant_nursery`, `vineyard`
7. **Natural**: Represents undeveloped or natural areas. OSM tags: `natural=*`, `leisure=nature_reserve` or `landuse` with values including: `beach` or `forest`.
8. **Other Land Uses**: A catch-all category for miscellaneous land use classes that do not fit into the categories above.


### Land Consumption Output Variables

The basic and detailed tables as well as the treemap include the following three output variables:

**% of Consumed Land Area**
- The proportion of consumed land taken up by each land use object.
- Example: Buildings account for 35.38% of consumed land in Bergheim, Heidelberg.

**% of Settled Land Area**
- The proportion of settled land occupied by each land use object.
- Example: Buildings account for 22.94% of settled land in Bergheim, Heidelberg.


### Data
The Land Consumption plugin is based on [OSM](https://www.openstreetmap.org/about) data.
OSM is a free and open geo-database with rich cartographic information about the built and natural environment.
