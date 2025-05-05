A table with proportion of land consumed in a given area of interest. Consuming land in this context refers to the
total area taken up by buildings, streets, and other built up areas. The basic output table reports how much land has
been consumed by land use object and the detailed output table shows how much has been consumed by land use class and land use object.

We consider the following land use objects and classes for the basic table:
- Buildings: OSM tag `building=*`
- Parking Lots: OSM tags `amenity=parking` and `parking=surface`. This does not include areas used for parking on the side of the street nor parking buildings.
- Roads: OSM tags `highway=*`.
- Other built up areas: OSM tag `land_use` equal to one of the following `residential`, `garages`, `railway`, `industrial`, `commercial`, `agric`, `retail`, `harbour`, `port`, `lock`, `marina`, OR tag `amenity` equal to `university` or `school`

For roads, we use tag `width` to estimate their area. When a road does not have the `width` tag, we assign it the most common width value (i.e., the mode) of its respective road type (`highway` tag value) in the area of interest.

For the detailed table the same land use objects are used but with each land use object (building, road, or parking lot) split by land use class (residential, commercial, industrial, agricultural, and other).
The following land use tags are used to classify each land use class.
- Residential areas: OSM tag `land_use` = `residential`
- Commercial areas: OSM tag `land_use` = `commercial` or - `retail`
- Industrial areas: OSM tag `land_use` = `industrial`
- Agriculture areas: OSM tag `land_use` equal to one of the following `allotments`, `farmland`, `farmyard`, `meadow`, `orchard`, `orchard`, `plant_nursery`, `vineyard`
- Other: catch-all land use class for miscellaneous land use classes that fall outside the above categories.