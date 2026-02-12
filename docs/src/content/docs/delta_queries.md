---
title: Delta queries
description: Track entity changes over time with single-delta and cross-delta queries
---

Delta queries track changes in entities over time, returning creation, modification, and deletion information.

Only SELECT queries are allowed.

## Single-delta structured query

A single-delta query runs on a specific change instance of the dataset, focusing on differences between two versions.

Instantiate `DeltaQuery` with the SPARQL query string, the time of interest, an optional set of properties, and the configuration file path:

```python
from time_agnostic_library.agnostic_query import DeltaQuery

agnostic_entity = DeltaQuery(
    query=QUERY_STRING,
    on_time=(START, END),
    changed_properties=PROPERTIES_SET,
    config_path=CONFIG_PATH
)
agnostic_entity.run_agnostic_query()
```

- The query string identifies the entities whose changes you want to investigate
- The time is a tuple `(START, END)`. If one value is `None`, only the other is considered. Dates must be in ISO 8601 format (e.g., `2023-01-01`, `2023-01-01T00:00:00+00:00`)
- The set of properties narrows results to entities where those specific properties changed. If empty, any changes are considered

The output reports modified entities with creation, modification, and deletion timestamps. Changes are reported as SPARQL UPDATE queries:

```python
{
    RES_URI_1: {
        "created": TIMESTAMP_CREATION,
        "modified": {
            TIMESTAMP_1: UPDATE_QUERY_1,
            TIMESTAMP_2: UPDATE_QUERY_2,
            TIMESTAMP_N: UPDATE_QUERY_N
        },
        "deleted": TIMESTAMP_DELETION
    },
    RES_URI_2: {
        "created": TIMESTAMP_CREATION,
        "modified": {
            TIMESTAMP_1: UPDATE_QUERY_1,
            TIMESTAMP_2: UPDATE_QUERY_2,
            TIMESTAMP_N: UPDATE_QUERY_N
        },
        "deleted": TIMESTAMP_DELETION
    }
}
```

## Cross-delta structured query

A cross-delta query runs across the entire history of the dataset, allowing for evolution studies.

Instantiate `DeltaQuery` without the `on_time` parameter:

```python
from time_agnostic_library.agnostic_query import DeltaQuery

agnostic_entity = DeltaQuery(
    query=QUERY_STRING,
    changed_properties=PROPERTIES_SET,
    config_path=CONFIG_PATH
)
agnostic_entity.run_agnostic_query()
```

The output follows the same format. If the entity was not created or deleted within the indicated range, the `created` or `deleted` value is `None`. If the entity does not exist within the input interval, the `modified` value is an empty dictionary:

```python
{
    RES_URI_1: {
        "created": TIMESTAMP_CREATION,
        "modified": {
            TIMESTAMP_1: UPDATE_QUERY_1,
            TIMESTAMP_2: UPDATE_QUERY_2,
            TIMESTAMP_N: UPDATE_QUERY_N
        },
        "deleted": TIMESTAMP_DELETION
    },
    RES_URI_2: {
        "created": TIMESTAMP_CREATION,
        "modified": {
            TIMESTAMP_1: UPDATE_QUERY_1,
            TIMESTAMP_2: UPDATE_QUERY_2,
            TIMESTAMP_N: UPDATE_QUERY_N
        },
        "deleted": TIMESTAMP_DELETION
    }
}
```
