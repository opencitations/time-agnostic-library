---
title: Delta queries
description: Compute net deltas between versions with single-delta and cross-delta queries
---

Delta queries compute the net difference between entity versions, returning added and removed triples along with creation and deletion timestamps.

Only SELECT queries are allowed.

## Single-delta structured query

A single-delta query computes the net delta between two versions of the dataset, identified by a time interval.

Instantiate `DeltaQuery` with the SPARQL query string, the time of interest, an optional set of properties, and either a configuration file path or a configuration dictionary:

```python
from time_agnostic_library.agnostic_query import DeltaQuery

delta = DeltaQuery(
    query=QUERY_STRING,
    on_time=(START, END),
    changed_properties=PROPERTIES_SET,
    config_path=CONFIG_PATH
)
delta.run_agnostic_query()
```

### Parameters

- `query` (`str`): a SPARQL SELECT query that identifies the entities whose changes you want to investigate
- `on_time` (`tuple[str | None, str | None]`, optional): the time interval `(START, END)`. If one value is `None`, only the other is considered. Dates must be in ISO 8601 format (e.g., `2023-01-01`, `2023-01-01T00:00:00+00:00`)
- `changed_properties` (`set[str]`, optional): narrows results to entities where the specified properties changed. If empty or `None`, any changes are considered
- `config_path` (`str`, default `"./config.json"`): path to a JSON configuration file
- `config_dict` (`dict`, optional): pass a configuration dictionary directly instead of using `config_path`

The output reports, for each entity, the net triples added and removed between the two versions. The delta is computed by composing the stored SPARQL UPDATE queries in the interval, without materializing any version state:

```python
{
    RES_URI_1: {
        "created": TIMESTAMP_CREATION,
        "deleted": TIMESTAMP_DELETION,
        "additions": {(subject, predicate, object, graph), ...},
        "deletions": {(subject, predicate, object, graph), ...}
    },
    RES_URI_2: {
        "created": TIMESTAMP_CREATION,
        "deleted": TIMESTAMP_DELETION,
        "additions": {(subject, predicate, object, graph), ...},
        "deletions": {(subject, predicate, object, graph), ...}
    }
}
```

Each element of `additions` and `deletions` is a tuple of N3-encoded strings representing a quad `(subject, predicate, object, graph)`.

## Cross-delta structured query

A cross-delta query computes the net delta across the entire history of the dataset.

Instantiate `DeltaQuery` without the `on_time` parameter:

```python
from time_agnostic_library.agnostic_query import DeltaQuery

delta = DeltaQuery(
    query=QUERY_STRING,
    changed_properties=PROPERTIES_SET,
    config_path=CONFIG_PATH
)
delta.run_agnostic_query()
```

The output follows the same format. If the entity was not created or deleted within the indicated range, the `created` or `deleted` value is `None`. If no triples changed for a given property filter, `additions` and `deletions` are empty sets:

```python
{
    RES_URI_1: {
        "created": TIMESTAMP_CREATION,
        "deleted": TIMESTAMP_DELETION,
        "additions": {(subject, predicate, object, graph), ...},
        "deletions": {(subject, predicate, object, graph), ...}
    }
}
```
