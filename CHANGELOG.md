# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [Unreleased]
Completely revamped `bagtiler` module
### Changed
- the `bagtiler` module is now generic and not hard-coded to BAG
- rename `bagtiler` module to `footprints`
- rename `bagtiler()` -> `create_views()`; `create_tile_edges() -> `update_tile_index()`, `create_centroid_table()` -> `create_centroids()`

### Added
- `footprints.partition()` for one-step footprint partitioning
- unit tests for `footprints`

## [0.3.1] - 2017-07-17
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
[0.3.0]: https://github.com/balazsdukai/batch3dfier/releases/tag/v0.3.0
[0.2.0]: https://github.com/balazsdukai/batch3dfier/releases/tag/v0.2
[0.1.0]: https://github.com/balazsdukai/batch3dfier/releases/tag/v0.1

