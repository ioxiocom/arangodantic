# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] - 2022-08-05

### Changed

- Switch from Travis CI to GitHub actions.

### Added

- Add back support for Python 3.7.

## [0.3.0] - 2022-08-04

### Removed

- Remove support for Python 3.7.

### Changed

- Update dependencies.
- Updates due to rename of GitHub organization.

## [0.2.0] - 2021-06-22

### Added

- Add `sort` parameter to `find` and `find_one` methods in models.
- Add `full_count` property to `ArangodanticCursor`.
- Add `offset` parameter to be used together with `limit` in `find`.
- Add `from_key_` and `to_key_` properties to edges.

### Changed

- Saving (especially edge models) through the graph can now raise a `ModelNotFoundError`
  instead of `DocumentInsertError` in certain cases.
- Update `pydantic` to ^1.8.2 that fixes CVE-2021-29510.

## [0.1.0] - 2020-10-27

### Added

- Add "find" and "find_one" methods to models.
- Add "truncate_collection" method to models.

### Changed

- The "delete_collection" method now raises DataSourceNotFound if the collection is
  missing and ignore_missing is set to False.
- Bump version to 0.1.0, project seems to be stable enough and has enough features for
  that.

## [0.0.4] - 2020-10-21

### Added

- Initial support for graphs

### Changed

- Renamed _\_before_save_ to _before_save_
- Bump version

## [0.0.3] - 2020-10-14

### Changed

- Update README.md
- Support for unique constraint errors
- Refactor handling of locks
- Bump version

## [0.0.2] - 2020-10-09

### Changed

- Update deployment credentials
- Bump version

## [0.0.1] - 2020-10-09

### Added

- Initial release of Arangodantic

[unreleased]: https://github.com/ioxiocom/arangodantic/compare/0.3.1...HEAD
[0.3.1]: https://github.com/ioxiocom/arangodantic/compare/0.3.0...0.3.1
[0.3.0]: https://github.com/ioxiocom/arangodantic/compare/0.2.0...0.3.0
[0.2.0]: https://github.com/ioxiocom/arangodantic/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/ioxiocom/arangodantic/compare/0.0.4...0.1.0
[0.0.4]: https://github.com/ioxiocom/arangodantic/compare/0.0.3...0.0.4
[0.0.3]: https://github.com/ioxiocom/arangodantic/compare/0.0.2...0.0.3
[0.0.2]: https://github.com/ioxiocom/arangodantic/compare/0.0.1...0.0.2
[0.0.1]: https://github.com/ioxiocom/arangodantic/releases/tag/0.0.1
