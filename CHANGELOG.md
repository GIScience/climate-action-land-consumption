# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project mostly adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/compare/1.0.4...main)

### Added

- added ohsome filter function for leaner ohsome py requests
- Added note about OSM data quality to methodology and artifact summaries ([#64](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/issues/64))

### Changed
- updated climatoology to v7.0.0

## [1.0.4](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/releases/1.0.4) - 2025-10-24

### Added

- added tests for ohsome-py data connection and warn if using other data
  connections ([#60](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/issues/60))

### Fixed

- explode geometries before `make_valid` to avoid mixed-dimension
  inputs ([#61](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/issues/61))

## [1.0.3](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/releases/1.0.3) - 2025-10-23

### Fixed

- fixed bug with ohsome-py filter for checking path count, not yet tested ([#60](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/issues/60))

## [1.0.2](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/releases/1.0.2) - 2025-10-22

### Fixed

- fixed bug with invalid OSM geometries from ohsome-py query ([#59](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/issues/59))
- changed road length to path count prior to ohsome-py query

## [1.0.1](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/releases/1.0.1) - 2025-10-17

### Added

- two more backends for querying the OSM data, in addition to the previous parquet approach: parquet via DuckDB, and
  ohsome-py. ohsome-py is the default option, while the other options can be selected with the `BACKEND` env variable.
  This addresses ([#54](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/issues/54))

## [1.0.0](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/releases/1.0.0)

### Added

- add computation limit based on road length to abort computations likely to fail
- add % of Total Land Area Column
- add institutional and infrastructure land use classes
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

- change all artifacts to primary
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
- fix crash on improperly clipped geometries

### Removed

- dummy table was removed

## [Dummy](https://gitlab.heigit.org/climate-action/plugins/land-consumption/-/releases/Dummy)
- output dummy table output with the percentage of an area of interest consumed by different land uses
