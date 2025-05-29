# [5.0.0](https://github.com/opencitations/time-agnostic-library/compare/4.7.1...5.0.0) (2025-05-29)


### Features

* Add script to find representative entities for benchmarking ([c457cee](https://github.com/opencitations/time-agnostic-library/commit/c457ceedcda721406dfb12ad5974d804e4d45066))
* **agnostic_entity:** remove cache and enhance entity relationships ([b9258b7](https://github.com/opencitations/time-agnostic-library/commit/b9258b768d082907696ec5720bfe70e529021255))
* Enhance entity selection script with local file analysis and improved SPARQL querying ([bd5d871](https://github.com/opencitations/time-agnostic-library/commit/bd5d8714d305b913220d36e290d4e15c411407b7))


### BREAKING CHANGES

* **agnostic_entity:** - Added new parameters to AgnosticEntity:
  - `include_related_objects`: Enables inclusion of related objects
  - `include_merged_entities`: Controls inclusion of merged entities
  - `include_reverse_relations`: Manages reverse relationship inclusion
- Removed all caching functionality as it was deemed obsolete and non-functional
- Increased test coverage for new features
- Removed untested and deprecated features

This change requires updates to all code that instantiates or interacts with AgnosticEntity objects.

[release]

## [4.7.1](https://github.com/opencitations/time-agnostic-library/compare/4.7.0...4.7.1) (2025-04-17)


### Bug Fixes

* updated the metadata structure to initialize `wasDerivedFrom` as an empty list and populate it with derived entity URIs. ([f5ac68f](https://github.com/opencitations/time-agnostic-library/commit/f5ac68f1e70209dc750f796e46e8625554352ba0))

# [4.7.0](https://github.com/opencitations/time-agnostic-library/compare/4.6.15...4.7.0) (2025-04-17)


### Features

* [release] include merged entities in history retrieval ([a2f186a](https://github.com/opencitations/time-agnostic-library/commit/a2f186a262427ee7cd0dafc6b10333c0ab995071))

## [4.6.15](https://github.com/opencitations/time-agnostic-library/compare/4.6.14...4.6.15) (2025-03-12)


### Bug Fixes

* [release] removed unnecessary dependencies ([5d5313a](https://github.com/opencitations/time-agnostic-library/commit/5d5313afa68401df080124bc7ca96d6ea25a68c9))

## [4.6.14](https://github.com/opencitations/time-agnostic-library/compare/4.6.13...4.6.14) (2025-02-28)


### Bug Fixes

* [release] ([7d7c30d](https://github.com/opencitations/time-agnostic-library/commit/7d7c30d435f69eae771d2516c3a6e298c25aec68))
* Added workflow for automatic releases and removed dependency on rdflib-ocdm. ([8d01a82](https://github.com/opencitations/time-agnostic-library/commit/8d01a82d57c754fd5c6ceb81bbbab6aea1e909c8))
* **ci:** I have updated the version of the action "upload artifact" to version 4 because version 3 was deprecated. ([65fdcef](https://github.com/opencitations/time-agnostic-library/commit/65fdcef36f2d0692e8beb35a6ed3d982c1322e4b))
* **ci:** remove Python 3.8 support due to Poetry incompatibility ([54a2754](https://github.com/opencitations/time-agnostic-library/commit/54a2754cb22819597ac9a616b239f3b13a368136))
* **ci:** use BYOB for coverage badge generation ([c47da23](https://github.com/opencitations/time-agnostic-library/commit/c47da2338913cd73a36e5500f6e4955477de3d67))
* update tagFormat in .releaserc to match existing version tags ([25bb1ae](https://github.com/opencitations/time-agnostic-library/commit/25bb1ae514fbd544274025c154f75fc2b8b5563f))

## [1.0.1](https://github.com/opencitations/time-agnostic-library/compare/v1.0.0...v1.0.1) (2025-02-28)


### Bug Fixes

* [release] ([7d7c30d](https://github.com/opencitations/time-agnostic-library/commit/7d7c30d435f69eae771d2516c3a6e298c25aec68))

# 1.0.0 (2025-02-28)


### Bug Fixes

* Added workflow for automatic releases and removed dependency on rdflib-ocdm. ([8d01a82](https://github.com/opencitations/time-agnostic-library/commit/8d01a82d57c754fd5c6ceb81bbbab6aea1e909c8))
* **ci:** I have updated the version of the action "upload artifact" to version 4 because version 3 was deprecated. ([65fdcef](https://github.com/opencitations/time-agnostic-library/commit/65fdcef36f2d0692e8beb35a6ed3d982c1322e4b))
* **ci:** remove Python 3.8 support due to Poetry incompatibility ([54a2754](https://github.com/opencitations/time-agnostic-library/commit/54a2754cb22819597ac9a616b239f3b13a368136))
* **ci:** use BYOB for coverage badge generation ([c47da23](https://github.com/opencitations/time-agnostic-library/commit/c47da2338913cd73a36e5500f6e4955477de3d67))

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [4.6.13] - Current Version

### Added
- Initial changelog creation for semantic-release integration
