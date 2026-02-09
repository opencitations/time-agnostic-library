# OSTRICH multi-snapshot strategies

## Source code
- Branch `feat/olivier` in `rdfostrich/ostrich` (not merged to master)
- PR #5 by Olivier Pelgrin: "Full support for multiple snapshots and compressed versioning metadata"
- Merged 2023-09-15

## Available strategies (CLI names)
| CLI name | Class | Parameter | Paper name |
|---|---|---|---|
| `never` | NeverCreateSnapshot | - | Baseline (original OSTRICH) |
| `interval N` | CreateSnapshotEveryN | N (period) | Periodic (d=N) |
| `change T` | ChangeRatioCreationStrategy | T (threshold) | CR (gamma=T) |
| `time R` | TimeCreationStrategy | R (ratio) | Time (theta=R) |
| `size T` | SizeCreationStrategy | T | - |
| `size2 T` | SizeCreationStrategy2 | T | - |
| `aggchange T` | AggregatedChangeRatioStrategy | T | - |
| `locaggchange T` | LocalAggregatedChangeRatioStrategy | T | - |

Composite strategies supported via `OR_CompositeSnapshotStrategy` and `AND_CompositeSnapshotStrategy`.

## Key files in feat/olivier
- `src/main/cpp/controller/snapshot_creation_strategy.h` - strategy interface and implementations
- `src/main/cpp/controller/snapshot_creation_strategy.cc` - factory `get_strategy(name, param)`
- `src/main/cpp/evaluate/evaluator.cc` - benchmark evaluator
- `src/main/cpp/snapshot/snapshot_manager.cc` - snapshot management

## Pelgrin paper configurations (BEAR-B)
From Pelgrin et al. 2025 (swj3625/swj3940):

| Method | BEAR-B Daily | Hourly | Instant |
|---|---|---|---|
| Baseline (never) | 1 snapshot | 1 | cannot complete |
| Periodic d=5 | 17 snapshots | - | - |
| Periodic d=10 | 8 | - | - |
| Periodic d=100 | - | 12 | 2104 |
| Periodic d=500 | - | - | 42 |
| CR gamma=2.0 | 5 | 23 | 151 |
| CR gamma=4.0 | 3 | 16 | 98 |
| Time theta=2.0 | 3 | 26 | 293 |

## BEAR-B-instant limitations
- OSTRICH baseline (`never`) cannot complete ingestion for instant (21,046 versions)
- Multi-snapshot strategies required for instant
- Paper results are only in graph form (no tables with exact average values)
- VM: ~20-120 us, DM: ~20-120 us, VQ: up to 1.75 x 10^6 us (~1.75s)

## Literature coverage for BEAR-B-instant
- OSTRICH original (Taelman 2018): NOT evaluated ("requires increasingly more time for each new version ingestion")
- GLENDA (Pelgrin, ESWC 2023): NOT evaluated (only BEAR-A)
- Pelgrin et al. 2025: YES, with multi-snapshot strategies only, results as graphs only
- Fernandez et al. (Jena, HDT): NOT evaluated for instant
- TrieDF / v-RDFCSA (Cerdeira-Pena): NOT found for instant

## How to use in setup_ostrich.sh
Change git clone to use `feat/olivier` branch:
```bash
git clone --recurse-submodules -b feat/olivier https://github.com/rdfostrich/ostrich.git
```
Then pass strategy to ingestion:
```bash
docker run ... ostrich ingest <strategy> <param> /var/patches <start> <end>
```
Example: `ingest change 4.0 /var/patches 1 21046` for CR gamma=4.0
