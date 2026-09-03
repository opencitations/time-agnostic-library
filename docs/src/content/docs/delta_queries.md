---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Delta queries
description: Compare SPARQL solution mappings between versions
---

Delta queries compare the bags of solution mappings that a SPARQL query returns at different times. They follow the same rules as `VersionQuery`, which supports basic graph patterns, OPTIONAL clauses, DISTINCT, and the inverse of one predicate.

```python
from time_agnostic_library.agnostic_query import DeltaQuery

delta = DeltaQuery(
    query=QUERY_STRING,
    on_time=(START, END),
    merge_aware=True,
    include_prov_metadata=True,
    config_path=CONFIG_PATH,
)
results, provenance, other_provenance = delta.run_agnostic_query()
```

Omit `on_time` to cover the full history, or set either bound to `None` when the interval has one open end. With `merge_aware=True`, the query also reads entity histories connected by merges, while the `merges` field records how their IRIs are connected to the IRI used in the query.

Use a closed range when you need the net change between two known dates. Use the full history when you need to see every recorded step.

```python
{
    "additions": [SOLUTION_MAPPING, ...],
    "deletions": [SOLUTION_MAPPING, ...],
    "merges": [
        {
            "time": TIMESTAMP,
            "snapshot": SNAPSHOT_IRI,
            "survivor": ENTITY_IRI,
            "absorbed": [ENTITY_IRI, ...],
        }
    ],
    "changes": [
        {
            "start": TIMESTAMP,
            "end": TIMESTAMP,
            "additions": [SOLUTION_MAPPING, ...],
            "deletions": [SOLUTION_MAPPING, ...],
        }
    ],
}
```

The top-level lists contain the multiset difference between the interval endpoints, so duplicate mappings keep their count unless the query uses DISTINCT. If the same row occurs twice, the bag records both copies. Each item in `changes` compares two consecutive states, identifies both timestamps, and appears in time order. Because the output uses normal query rows, you can read each value in the same way as a `VersionQuery` result.

When `merge_aware` is enabled, `merges` contains every merge event used to connect the query IRIs to the entity histories that were evaluated. This includes events before the requested interval because they can explain why a different historical IRI contributed a solution. The field is `[]` when no merge was found and `None` when merge handling is disabled.
