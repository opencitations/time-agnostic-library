---
title: Delta queries
description: Track entity changes over time with single-delta and cross-delta queries
---

Delta queries track changes in entities over time, returning creation, modification, and deletion information.

Only SELECT queries are allowed.

## Single-delta structured query

A single-delta query runs on a specific change instance of the dataset, focusing on differences between two versions.

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

delta = DeltaQuery(
    query=QUERY_STRING,
    changed_properties=PROPERTIES_SET,
    config_path=CONFIG_PATH
)
delta.run_agnostic_query()
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
