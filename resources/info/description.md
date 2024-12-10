A table with proportion of land consumed in a given area of interest. Consuming land in this context refers to the
sealing of soil with impervious surfaces. The output table reports how much soil in an area has been sealed.

We consider the following land use classes:
- Buildings: OSM tag `building=*`
- Parking Lots: OSM tags `amenity=parking` and `parking=surface`. This does not include areas used for parking on the side of the street nor parking buildings.
- Paved Roads: OSM tags `highway=*` and `surface` tags corresponding to paved surfaces. When the road surface is not available, we assume the road is paved.
- Unpaved Roads: OSM tags `highway=*` and `surface` tags corresponding to unpaved surfaces.
- Other built up areas: OSM tag `land_use` equal to one of the following `residential`, `garages`, `railway`, `industrial`, `commercial`, `retail`, `harbour`, `port`, `lock`, `marina`, OR tag `amenity` equal to `university` or `school`

For roads, we use tag `width` to estimate their area. When a road does not have the `width` tag, we assign it the most common width value (i.e., the mode) of its respective road type (`highway` tag value) in the area of interest.

Soil sealing factors are based on values provided by the Austrian Federal Environmental Agency (https://www.umweltbundesamt.at/umweltthemen/boden/flaecheninanspruchnahme/definition-flaechen).