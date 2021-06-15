# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2021-06-16

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
- The "delete_collection" method now raises DataSourceNotFound if the
collection is missing and ignore_missing is set to False.
- Bump version to 0.1.0, project seems to be stable enough and has enough
features for that.

## [0.0.4] - 2020-10-21

### Added
- Initial support for graphs

### Changed
- Renamed *_before_save* to *before_save*
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

[Unreleased]: https://github.com/digitalliving/arangodantic/compare/0.2.0...HEAD
[0.2.0]: https://github.com/digitalliving/arangodantic/compare/0.1.0...0.2.0
[0.1.0]: https://github.com/digitalliving/arangodantic/compare/0.0.4...0.1.0
[0.0.4]: https://github.com/digitalliving/arangodantic/compare/0.0.3...0.0.4
[0.0.3]: https://github.com/digitalliving/arangodantic/compare/0.0.2...0.0.3
[0.0.2]: https://github.com/digitalliving/arangodantic/compare/0.0.1...0.0.2
[0.0.1]: https://github.com/digitalliving/arangodantic/releases/tag/0.0.1
