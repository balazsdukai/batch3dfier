# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
- Import the contents of CSV-BUILDINGS-MULTIPLE into a postgres table that can be joined on the footprints
- Log file (just basic info)
- Python wheel

### Changed
- `batch3dfy` console script -> `batch3dfier`

## [0.6.0] - 2017-08-08
### Added
- support for python 3.4-3.6
- code coverage reporting
- pep8 tests

## [0.5.0] - 2017-07-28
### Changed
- the `tile_index` argument block is updated to handle both footprint and pointcloud tile indexes
- db_setup.sh to reflect documentation

### Added
- `tile_prefix` to indicate the prefix on footprint tile views
- Find intersecting pointcloud tiles for 2D footprint. Local directory is still searched for corresponding pointcloud files.
- `uniqueid` config argument
- detailed documentation

### Removed
- ID/name match between 2D and pointcloud tiles, because the same tile index is used for both datasets.
- hard-coded BAG parameters

## [0.4.0] - 2017-07-18
Completely revamped `bagtiler` module
### Changed
- the `bagtiler` module is now generic and not hard-coded to BAG
- rename `bagtiler` module to `footprints`
- rename `bagtiler()` -> `create_views()`; `create_tile_edges() -> `update_tile_index()`, `create_centroid_table()` -> `create_centroids()`
- README update

### Added
- `footprints.partition()` for one-step footprint partitioning
- unit tests for `footprints`

## [install-update] - 2017-07-17
### Changed
- README install/run instructions

## [0.3.0] - 2017-07-17
### Added
- -t/--threads argument to set the number of concurrent processes
- utf-8 encoding
- `batch3dfy` entry point for console
- package setup.py
- CHANGELOG

### Changed
- -c argument is now positional
- batch3dfy.py -> batch3dfierapp.py including main()
- sanitized script headers
- README to reflect packaging

## [0.2.0] - 2017-07-02
### Added
- `bagtiler` module for setting up a BAG databse

### Fixed
- fixed Queue limit for threads

## [0.1.0] - 2017-06-21
### Added
- Extrude polygons in extend.
- Extrude polygons in all provided tiles.
- Hard-coded BAG database fields (2D polygons).
- ID/name match between 2D and pointcloud tiles (thus no spatial search), because the same tile index is used for both datasets.


[Unreleased]: https://github.com/balazsdukai/batch3dfier/tree/develop
[0.7.0]: https://github.com/balazsdukai/batch3dfier/releases/tag/v0.7.0
[0.6.0]: https://github.com/balazsdukai/batch3dfier/releases/tag/v0.6.0
[0.5.0]: https://github.com/balazsdukai/batch3dfier/releases/tag/v0.5.0
[0.4.0]: https://github.com/balazsdukai/batch3dfier/releases/tag/v0.4.0
[install-update]: https://github.com/balazsdukai/batch3dfier/releases/tag/install-update
[0.3.0]: https://github.com/balazsdukai/batch3dfier/releases/tag/v0.3.0
[0.2.0]: https://github.com/balazsdukai/batch3dfier/releases/tag/v0.2
[0.1.0]: https://github.com/balazsdukai/batch3dfier/releases/tag/v0.1

