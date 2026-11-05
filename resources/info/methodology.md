The **Land Consumption** assessment tool quantifies how land is used and consumed in a given an area.

It measures what fraction of the AOI is covered by different land use objects (roads, buildings, etc.) and land use classes (residential, commercial, etc.). The tool then reports a detailed composition of land use (e.g., how much land is used for buildings in residential areas versus roads in commercial areas).

All inputs for this tool are based on OpenStreetMap data.

### Definitions

**Settled Land:**
Includes all land that is human-altered or developed such as agriculture, buildings, and paved areas, but excludes natural land.

**Consumed Land:**
Buildings, roads, parking areas, and other built infrastructure. It does not include agricultural, natural, or semi-natural areas.

**Built-up Land:**
Includes all land that is within an area tagged in OSM as commercial, residential, industrial, infrastructure, institutional, or other human-altered land uses but not directly assigned to a tag itself. In essence, built-up land refers to the interstitial spaces between other tags in consumed or settled areas. For example, this includes the land surrounding a building in a commercial area that is not tagged as a sidewalk or another feature.


### Land Consumption Output Variables

The core output variables of the basic and detailed tables as well as the treemap include the following two output variables:

**% of Consumed Land Area**
- The proportion of consumed land taken up by each land use object.
- Example: Buildings account for 35.38% of consumed land in Bergheim, Heidelberg.

**% of Settled Land Area**
- The proportion of settled land occupied by each land use object.
- Example: Buildings account for 22.94% of settled land in Bergheim, Heidelberg.

### Land Use Object and Class Identification with OSM

The Land Consumption plugin currently considers the following land use objects when estimating land consumption in a given area. Some of these objects such as Agricultural Land are not objects themselves, but are included in the object category in order to understand how land is being used and consumed.

1. **Buildings**: Tagged as `building=*` in OpenStreetMap (OSM).
2. **Parking Lots**: Identified using the tags `amenity=parking` and `parking=surface`. Note: This excludes multi-level parking structures and on-street parking.
3. **Roads**: Tagged as `highway=*`. Road area is calculated using the width tag. If the width is not available, the most common width for the respective road type in the area (based on its highway tag) is used.
4. **Built-up Land**: Refers to areas immediately surrounding OSM objects with `landuse` tags such as `residential`, `commercial`, or `industrial`. These are not discrete objects like buildings or roads, but include surfaces such as paved areas, courtyards, and plazas.
5. **Agricultural Land**: Includes land tagged as `landuse` with values including: `allotment`, `farmland`, `farmyard`, `meadow`, `orchard`, `plant_nursery`, `vineyard`, `greenhouse_horticulture`. Agricultural land is excluded from consumed land calculations.
6. **Natural land**: Represents undeveloped or natural areas tagged as `natural=*`, `leisure=nature_reserve` or `landuse=forest`. Also includes natural water features such as `lake`or `river`. Natural land is excluded from both consumed and settled land calculations.
7. **Other**: A catch-all category for land use types not covered by the above classes. Other is excluded from consumed land calculations.
8. **Unknown**: Includes all OSM tagged objects not included in the above categories. Unknown is excluded from consumed land calculations.

In addition to land use objects, the Land Consumption plugin also considers the following land use classes in its calculations:

1. **Residential**: Includes land primarily used for housing and dwellings. OSM landuse value: `residential`.
2. **Commercial**: Includes land used for business, retail, and service-related functions. OSM landuse values: `commercial`, `retail`.
3. **Industrial**: Includes land designated for manufacturing, warehousing, or other industrial activities. OSM landuse value: `industrial`.
4. **Infrastructure**: Covers land used for public infrastructure such as transport. OSM landuse values: `garages`, `railway`, `harbour`, `port`, `lock`, `marina` or OSM amenity values: `bus_station`,`ferry_terminal` or OSM tag: `aeroway=aerodrome`.
5. **Institutional**: Covers land used for public infrastructure and institutional functions such as religious facilities and universities. OSM landuse values: `military`, `religious`, `cemetery`, `education` or OSM amenity values: `university`, `school`, `college`, `hospital`, `clinic`, `community_centre`,
 `courthouse`, `fire_station`, `police_station`, `prison`, `townhall`, `monastery`, `place_of_worship`.
6. **Agricultural**: Represents land used for farming, horticulture, and related rural activities. OSM landuse values: `allotments`, `farmland`, `farmyard`, `meadow`, `orchard`, `plant_nursery`, `vineyard`, `greenhouse_horticulture`.
7. **Natural**: Represents undeveloped or natural areas excluding water related features. OSM tags: `natural=*`, `leisure=nature_reserve` or OSM landuse value: `forest`.
8. **Water**: Represents all natural water features. OSM water values such as `lake`, `river` or `natural=bay`.
9. **Other Land Uses**: A catch-all category for miscellaneous land use classes that do not fit into the categories above.
10. **Unknown**: Includes all OSM tagged objects and land uses not included in the above categories.

### Data
The Land Consumption plugin is based on [OSM](https://www.openstreetmap.org/about) data.
OSM is a free and open geo-database with rich cartographic information about the built and natural environment.
OSM is created and maintained by volunteers.
If the data for your area of interest seem inaccurate and/or incomplete, you can help improve them by mapping your area in OSM.
To get started, check out the [OSM wiki](https://wiki.openstreetmap.org/wiki/Beginners%27_guide).
