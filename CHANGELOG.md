# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project mostly adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://gitlab.heigit.org/climate-action/plugins/land-consumption)

### Added

- add natural land class
- add detailed treemap artifact
- update to metadata and description
- added agriculture class to land use
- split basic and detailed reports into two different functions
- output changed to include both basic and detailed reports on land consumption
- detailed report calculates land consumption by land use classes (e.g. commercial) in addition to consumption by land use objects (e.g. buildings)
- updated documentation to reflect latest methodology
- calculate fraction of area of interest covered by buildings ([#8](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/issues/8))
- calculate land consumed by parking lots, adding one row to results table ([#9](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/issues/9))
- buffer the road LineStrings to its width or the mode (most used value) for that road type, if no value for a particular road type is found within AOI then use a default value, calculate area of the buffered roads, calculate land consumed by roads, adding one row to results table((#11)[https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/issues/11])


### Changed

- renamed other built up areas to infrastructural and institutional
- fixed double counting of osm features
- removed duplicate masking of agricultural class
- combined table sorting logic into one function
- properly assigned leftover agricultural land to its own class
- relabeled unknown class to other
- combined unpaved and paved road types into one class
- updated climatoology to v6.3.1
- include demo_input_parameters in operator worker file
- updated documentation to reflect latest methodology
- code modified for compatibility with climatoology 6.0.2
- request data as GeoParquet instead of Ohsome API

### Removed

- dummy table was removed

## [Dummy](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/releases/Dummy)
- output dummy table output with the percentage of an area of interest consumed by different land uses
