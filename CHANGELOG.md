# [6.0.0](https://github.com/opencitations/time-agnostic-library/compare/5.0.7...6.0.0) (2026-02-13)


* build!: migrate from Poetry to uv and drop Python 3.9 ([866c7e6](https://github.com/opencitations/time-agnostic-library/commit/866c7e6e6b7199d81935d2ed14da5a9de429dab1))
* refactor!: replace python-dateutil with datetime.fromisoformat ([7f2bf30](https://github.com/opencitations/time-agnostic-library/commit/7f2bf309df4e7508eae613aa5e882a3118e9bb8c))


### Bug Fixes

* correct version materialization and cross-version query results ([5434750](https://github.com/opencitations/time-agnostic-library/commit/54347509e6f8af400effe0d616fa57578ea77792))
* handle language-tagged literals and safe triple removal ([ed38d74](https://github.com/opencitations/time-agnostic-library/commit/ed38d749f4b32a6c317e275511363a74d7a9de36))
* prune stale SPARQL clients from dead executor threads ([1de40de](https://github.com/opencitations/time-agnostic-library/commit/1de40defa9156c838037b792a2a531acf722dce3))
* remove SPARQL client pooling that caused flaky CI failures ([b829dc4](https://github.com/opencitations/time-agnostic-library/commit/b829dc4929787a16148b2d5eb6b4afe0dcb1b00d))
* replace copy.deepcopy with _copy_dataset to prevent rdflib index corruption ([eccafba](https://github.com/opencitations/time-agnostic-library/commit/eccafba920227f4103850877e753bf3b4c36d40c))


### Features

* add BEAR benchmark suite with QLever backend ([49975fb](https://github.com/opencitations/time-agnostic-library/commit/49975fb71c1f0fa4fefe87c9264a27d79b952c50))
* add include_all_timestamps option to VersionQuery ([4d703a1](https://github.com/opencitations/time-agnostic-library/commit/4d703a1c4ea00d9a60188eb6e6f1cdcbc8a67da1))
* **benchmark:** add CLI arguments, resume support, and subprocess isolation for VQ queries ([743bac2](https://github.com/opencitations/time-agnostic-library/commit/743bac2fd1dcded90e5f32a1018d24e30ceb948e))
* **benchmark:** add multi-granularity support and comparison plots ([9d55845](https://github.com/opencitations/time-agnostic-library/commit/9d55845d2ed5e81ecd9536dd955ae3e5b62bab88))
* **benchmark:** add OSTRICH comparison and ingestion timing ([67844cc](https://github.com/opencitations/time-agnostic-library/commit/67844cc80c1d39207084a73157f330003c07f84b))
* **benchmark:** enable OSTRICH multi-snapshot ingestion strategies ([0e9942e](https://github.com/opencitations/time-agnostic-library/commit/0e9942eaf4ba87439d8c69280e6b78db097cf46f))
* extract OCDM converter into reusable library module ([9c8cee8](https://github.com/opencitations/time-agnostic-library/commit/9c8cee8d2eaaa07eea2227fec28a7d55a9d46b37))
* switch VersionQuery results to SPARQL JSON bindings format ([514c93d](https://github.com/opencitations/time-agnostic-library/commit/514c93d4dd8614d3ab8d0963771a42aba9fbeb7e))


### Performance Improvements

* batch SPARQL queries and set-based reconstruction for VM/VQ/DM ([8bd7bf1](https://github.com/opencitations/time-agnostic-library/commit/8bd7bf11d99728d505be54640ce955d42d3368b6))
* **converter:** optimize N-Triples parsing and I/O pipeline ([df713ba](https://github.com/opencitations/time-agnostic-library/commit/df713bad4580be9bf3750d5c2d39403b6cccdaf7))
* normalize typed literals for O(1) graph removal in update queries ([0ff6557](https://github.com/opencitations/time-agnostic-library/commit/0ff65576e3be9402b1201d625704caabf83ac0cc))
* reduce overhead from redundant copies, pyparsing, and sequential I/O ([4e9ff8f](https://github.com/opencitations/time-agnostic-library/commit/4e9ff8f6aae87997cf431f80df0dd36d1bfee790))
* remove dead code and use server-side entity discovery in DeltaQuery ([094f942](https://github.com/opencitations/time-agnostic-library/commit/094f94267d47705ada844248da63c4bbd8d45a3c))
* remove redundant DISTINCT from update query entity discovery ([c38c5f4](https://github.com/opencitations/time-agnostic-library/commit/c38c5f4851a4cdb9929aab067d77933c0fdd23b2))
* replace pyparsing-based SPARQL UPDATE parser with regex ([572ba47](https://github.com/opencitations/time-agnostic-library/commit/572ba474236be1af4fad635e8ab2c60a7feb364a))
* replace rdflib Dataset with N3 string sets for internal state ([3e698ac](https://github.com/opencitations/time-agnostic-library/commit/3e698ac84fa7211abbe09f59456b9200dcc1b5ac))
* replace ThreadPoolExecutor with ProcessPoolExecutor on Linux ([6271087](https://github.com/opencitations/time-agnostic-library/commit/62710873ea6602f51634b4cd1d2f2b892da6cdfb))
* simplify provenance query and remove DISTINCT from all internal queries ([beb91a7](https://github.com/opencitations/time-agnostic-library/commit/beb91a794d41c296aaa1e6be3bb739d98eba36d9))
* stream cross-version VersionQuery to avoid O(N) dataset copies ([15c0bb2](https://github.com/opencitations/time-agnostic-library/commit/15c0bb23ef99d3b382d1e0cedbdfaa93ed663a05))
* use set-based pattern matching for single-version VersionQuery ([ced44ca](https://github.com/opencitations/time-agnostic-library/commit/ced44ca607b6fa38cce2d6cc718dda280d11af54))
* use tuple instead of frozenset and skip single-timestamp alignment ([6d2117b](https://github.com/opencitations/time-agnostic-library/commit/6d2117b7942b5dda632737ea466515c7539c98b2))


### BREAKING CHANGES

* date/time values must now be in ISO 8601 format.
Non-ISO formats (e.g., "May 21, 2021") are no longer accepted.
* get_state_at_time returns empty entity_snapshots
dict when include_prov_metadata is False.
* Python 3.9 is no longer supported. Minimum required
version is now Python 3.10.

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
