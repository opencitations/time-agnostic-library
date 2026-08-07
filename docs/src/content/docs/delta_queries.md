---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Delta queries
description: Inspect net and per-snapshot deltas between versions
---

Delta queries return the changes to each matching entity over a time interval. They include the chronological snapshot changes, their net delta, creation and deletion times, and merge events.

The graph pattern follows the same restrictions as `VersionQuery`: basic graph patterns, OPTIONAL clauses, DISTINCT, and the inverse of one predicate are supported.

```python
from time_agnostic_library.agnostic_query import DeltaQuery

delta = DeltaQuery(
    query=QUERY_STRING,
    on_time=(START, END),
    changed_properties=PROPERTIES_SET,
    merge_aware=True,
    include_prov_metadata=True,
    config_path=CONFIG_PATH,
)
results, provenance, other_provenance = delta.run_agnostic_query()
```

Omit `on_time` to cover the full history. Either interval bound may be `None`. `changed_properties` filters additions and deletions by predicate.

`merge_aware` follows merges in both directions.

Each result record has this form:

```python
{
    ENTITY_IRI: {
        "created": TIMESTAMP_OR_NONE,
        "deleted": TIMESTAMP_OR_NONE,
        "changes": [
            {
                "time": TIMESTAMP,
                "additions": {(subject, predicate, object, graph), ...},
                "deletions": {(subject, predicate, object, graph), ...},
            }
        ],
        "additions": {(subject, predicate, object, graph), ...},
        "deletions": {(subject, predicate, object, graph), ...},
        "merges": [
            {
                "time": TIMESTAMP,
                "snapshot": SNAPSHOT_IRI,
                "survivor": ENTITY_IRI,
                "absorbed": [ENTITY_IRI, ...],
            }
        ],
    }
}
```

Quad terms use N3 strings. `changes` is chronological. The top-level additions and deletions contain the composed net delta for the interval. Creation and deletion times are `None` when the corresponding event falls outside the interval.

`merges` is `None` when merge support is disabled. It is `[]` when merge support is enabled and the entity has no merge event in the interval. A merge event appears in the records of its survivor and directly absorbed entities.
