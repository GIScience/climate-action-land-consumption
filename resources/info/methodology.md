Definition: land consumption is calculated as the proportion of artificial and modified land. Factors are assigned to calculate the degree of consumption.

Description: V1 of Land Consumption calculates the proportion of land consumed at one given time. The time is an input by the user and we associate it with the closest corine version in the past.

Required Input: a single date and bbox

Required Output: V1 will output text and a graph. Text will state proportion of land consumed and graph will state the proportion of land consumed by different land uses.

Methodology: Plugin works by taking date and bbox input to match to corine lulc. Corine data transformed by land consumption factors ranging 0 to 1 (0 unconsumed, 1 totally consumed). Factors come from WWF Austria. Consumption value for whole bbox averaged to get proportion consumed. This is the text output. We then iteratively calculate the proportion consumed by each land use and output these percentages to populate a graph.