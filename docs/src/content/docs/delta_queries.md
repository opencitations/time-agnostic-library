---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

title: Delta queries
description: Inspect net and per-snapshot deltas between versions
---

Delta queries return the net difference between entity versions and the chronological sequence of changes that produced it, along with creation and deletion timestamps.

Only SELECT queries are allowed, and their graph pattern must be made of basic graph patterns and OPTIONAL clauses. DISTINCT is accepted. Any other construct raises an error: FILTER, UNION, MINUS, GRAPH, BIND, aggregate functions, subqueries, VALUES, ORDER BY, LIMIT, OFFSET, and property paths other than the inverse of a single predicate.

## Delta structured query

A delta structured query operates on a time interval. It returns the chronological sequence of snapshot changes in that interval and their composed net delta. When `on_time=None`, the interval covers the entire dataset history.

Instantiate `DeltaQuery` with the SPARQL query string, an optional time interval and set of properties, and either a configuration file path or a configuration dictionary:

```python
from time_agnostic_library.agnostic_query import DeltaQuery

delta = DeltaQuery(
    query=QUERY_STRING,
    on_time=TIME_INTERVAL,
    changed_properties=PROPERTIES_SET,
    config_path=CONFIG_PATH
)
delta.run_agnostic_query()
```

### Parameters

- `query` (`str`): a SPARQL SELECT query that identifies the entities whose changes you want to investigate
- `on_time` (`tuple[str | None, str | None] | None`, default `None`): the time interval `(START, END)`. If one value is `None`, only the other bound is considered. If the interval is `None`, the entire dataset history is considered. Dates must be in ISO 8601 format (e.g., `2023-01-01`, `2023-01-01T00:00:00+00:00`)
- `changed_properties` (`set[str]`, optional): narrows results to entities where the specified properties changed. If empty or `None`, any changes are considered
- `config_path` (`str`, default `"./config.json"`): path to a JSON configuration file
- `config_dict` (`dict`, optional): pass a configuration dictionary directly instead of using `config_path`

The output reports, for each entity, the changes recorded at each snapshot in the interval and the net triples added and removed. The delta is computed from the stored SPARQL UPDATE queries without materializing any version state:

```python
{
    RES_URI_1: {
        "created": TIMESTAMP_CREATION,
        "deleted": TIMESTAMP_DELETION,
        "changes": [
            {
                "time": TIMESTAMP,
                "additions": {(subject, predicate, object, graph), ...},
                "deletions": {(subject, predicate, object, graph), ...}
            },
            ...
        ],
        "additions": {(subject, predicate, object, graph), ...},
        "deletions": {(subject, predicate, object, graph), ...}
    }
}
```

Each element of `additions` and `deletions` is a tuple of N3-encoded strings representing a quad `(subject, predicate, object, graph)`. The top-level sets contain the net delta for the interval. Each entry in `changes` contains the delta of one snapshot and its timestamp; entries are ordered chronologically. If the entity was not created or deleted within the interval, the corresponding `created` or `deleted` value is `None`. If no triples changed for a given property filter, the corresponding `additions` and `deletions` sets are empty.
