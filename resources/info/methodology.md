Definition: for now, land consumption is calculated as the fraction of soil that has been sealed with impervious surfaces, such as asphalt. Different land use types are weighted according to a land-consumption factors representing their average rate of soil sealing.

Description: this `demo` version of Land Consumption calculates the proportion of soil sealed based on the most recent OpenStreetMap (OSM) data.

Required Input: an area of interest.

Required Output: the demo outputs a table with the proportion of land consumed by different land uses.

Methodology: the plugin requests OSM data for the area of interest, calculates the area of different land use classes, and weighs them by their respective land consumption factors.