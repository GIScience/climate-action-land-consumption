Definition: for now, land consumption is calculated as the summed area of land use objects such as buildings, streets (paved and unpaved), and other built-up areas.
Description: this `demo` version of Land Consumption calculates the proportion of consumed area based on the most recent OpenStreetMap (OSM) data. It then splits land consumption by land use class for a detailed report on land consumption.

Required Input: an area of interest.

Required Output: the demo outputs two tables with the proportion of land consumed by different land use objects (e.g. buildings) and by land use class (e.g. commercial).

Methodology: the plugin requests OSM data for the area of interest, calculates the area of different land use classes and objects, and weighs them by their respective land consumption factors.