---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Structured queries
description: Run SPARQL SELECT queries on specific or all versions of the dataset
---

Structured queries run SPARQL SELECT queries against specific versions or all versions of the dataset.

The graph pattern can contain basic graph patterns and OPTIONAL clauses. DISTINCT and the inverse of one predicate are supported. Other SPARQL constructs raise an error.

## Single-version structured query

Pass the requested time interval through `on_time`:

```python
from time_agnostic_library.agnostic_query import VersionQuery

query = VersionQuery(
    query=QUERY_STRING,
    on_time=(START, END),
    merge_aware=True,
    include_prov_metadata=True,
    config_path=CONFIG_PATH,
)
results, provenance, other_provenance = query.run_agnostic_query()
```

The interval may represent an instant, a range, all states before a time, or all states after a time. Dates use ISO 8601. Either bound may be `None`.

`merge_aware` follows entity histories connected by merges. The query reconstructs each entity under the IRI used by that entity. A constant IRI in subject or object position matches all IRIs in the connected histories. Variables return the IRI stored in each historical state.

`include_prov_metadata` adds snapshot metadata. It does not enable merge support. `provenance` contains snapshots selected by `on_time`; `other_provenance` contains the remaining snapshots of the same entities. Both values are `None` when metadata is disabled. An enabled but empty channel is `{}`.

Metadata is grouped by entity and snapshot IRI:

```python
{
    ENTITY_IRI: {
        SNAPSHOT_IRI: {
            "generatedAtTime": TIMESTAMP,
            "invalidatedAtTime": TIMESTAMP_OR_NONE,
            "wasAttributedTo": AGENT_IRI,
            "hadPrimarySource": SOURCE_IRI_OR_NONE,
            "description": DESCRIPTION_OR_NONE,
            "hasUpdateQuery": UPDATE_QUERY_OR_NONE,
            "wasDerivedFrom": [SNAPSHOT_IRI, ...],
        }
    }
}
```

`results` maps timestamps to bindings in the [W3C SPARQL JSON results format](https://www.w3.org/TR/sparql11-results-json/):

```python
{
    TIME: [
        {
            "var1": {"type": "uri", "value": "http://example.com/res/1"},
            "var2": {"type": "literal", "value": "some text"},
        }
    ]
}
```

### Filling timestamp gaps

Results normally contain timestamps where a queried entity changed. `include_all_timestamps=True` also adds provenance timestamps where other entities changed:

```python
results, provenance, other_provenance = query.run_agnostic_query(
    include_all_timestamps=True
)
```

## Cross-version structured query

Omit `on_time` to query every version:

```python
query = VersionQuery(query=QUERY_STRING, config_path=CONFIG_PATH)
results, provenance, other_provenance = query.run_agnostic_query()
```

When metadata is enabled, every snapshot is in `provenance` and `other_provenance` is `{}`.
