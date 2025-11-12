## [5.0.7](https://github.com/opencitations/time-agnostic-library/compare/5.0.6...5.0.7) (2025-11-12)


### Bug Fixes

* use explicit triple addition for CONSTRUCT queries on Dataset [release] ([ff20047](https://github.com/opencitations/time-agnostic-library/commit/ff20047ba9f9c30f65c90f2afa1bc1282ff0c9fa))

## [5.0.6](https://github.com/opencitations/time-agnostic-library/compare/5.0.5...5.0.6) (2025-11-01)


### Bug Fixes

* **ci:** install Poetry and dev dependencies for documentation build ([884e332](https://github.com/opencitations/time-agnostic-library/commit/884e3329d4ac8176ec8f76b60d1397d6e849a7fd))
* migrate from deprecated ConjunctiveGraph to Dataset and add edge case tests for agnostic_entity, agnostic_query, and sparql modules ([49ecfac](https://github.com/opencitations/time-agnostic-library/commit/49ecfac27601618187284856756f5fe7e9f465fe))
* remove redundant and outdated edge case tests ([7c79179](https://github.com/opencitations/time-agnostic-library/commit/7c791798225989a477825f87c63dc48fb2cc5491))

## [5.0.5](https://github.com/opencitations/time-agnostic-library/compare/5.0.4...5.0.5) (2025-08-31)


### Bug Fixes

* **sparql:** [release] preserve original literal representation without explicit datatype ([b6915a8](https://github.com/opencitations/time-agnostic-library/commit/b6915a8ab3a5b089b8c24d254080a0a71acc39e3))

## [5.0.4](https://github.com/opencitations/time-agnostic-library/compare/5.0.3...5.0.4) (2025-07-26)


### Bug Fixes

* **agnostic_entity:** [release] add prov:specializationOf triples for targeted entity removal ([7c562f1](https://github.com/opencitations/time-agnostic-library/commit/7c562f10f8741525fa61c6587d1f58d1df813e79))

## [5.0.3](https://github.com/opencitations/time-agnostic-library/compare/5.0.2...5.0.3) (2025-07-07)


### Bug Fixes

* **agnostic_entity:** improve literal matching logic in query processing ([9f26506](https://github.com/opencitations/time-agnostic-library/commit/9f2650637c60ea1a18f797f5c8a2ee7ffd913b0d))

## [5.0.2](https://github.com/opencitations/time-agnostic-library/compare/5.0.1...5.0.2) (2025-05-30)


### Performance Improvements

* upgrade RDFLib to v7.1.4 and remove query locks ([d25fa38](https://github.com/opencitations/time-agnostic-library/commit/d25fa384056e13618cee95a04213aee856b339aa))

## [5.0.1](https://github.com/opencitations/time-agnostic-library/compare/5.0.0...5.0.1) (2025-05-29)


### Bug Fixes

* **agnostic_entity:** isolate recursive collection methods ([b51686d](https://github.com/opencitations/time-agnostic-library/commit/b51686d80a55693bb17016d980b7bedfeb5beb3f))

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
