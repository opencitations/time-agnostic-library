---
title: Structured queries
description: Run SPARQL SELECT queries on specific or all versions of the dataset
---

Structured queries allow running SPARQL SELECT queries against specific versions or all versions of the dataset.

Only SELECT queries are allowed.

## Single-version structured query

A single-version structured query runs a SPARQL query on a specified version. A version can be a time interval, an instant, a before, or an after.

Instantiate `VersionQuery` with the SPARQL query string, the time of interest, and either a configuration file path or a configuration dictionary, then call `run_agnostic_query()`:

```python
from time_agnostic_library.agnostic_query import VersionQuery

query = VersionQuery(query=QUERY_STRING, on_time=(START, END), config_path=CONFIG_PATH)
results, other_timestamps = query.run_agnostic_query()
```

### Parameters

- `query` (`str`): a SPARQL SELECT query that identifies the entities of interest
- `on_time` (`tuple[str | None, str | None]`, optional): the time interval `(START, END)`. If one value is `None`, only the other is considered. Dates must be in ISO 8601 format (e.g., `2023-01-01`, `2023-01-01T00:00:00+00:00`)
- `other_snapshots` (`bool`, default `False`): when `True`, the second element of the output contains timestamps of snapshots outside the requested time interval
- `config_path` (`str`, default `"./config.json"`): path to a JSON configuration file
- `config_dict` (`dict`, optional): pass a configuration dictionary directly instead of using `config_path`

The output is a tuple of two elements. The first is a dictionary where keys are timestamps and values are lists of bindings in the [W3C SPARQL JSON results format](https://www.w3.org/TR/sparql11-results-json/). The second is a set of other snapshot timestamps (empty unless `other_snapshots=True`):

```python
(
    {
        TIME: [
            {
                "var1": {"type": "uri", "value": "http://example.com/res/1"},
                "var2": {"type": "literal", "value": "some text"}
            },
            {
                "var1": {"type": "uri", "value": "http://example.com/res/2"},
                "var2": {"type": "literal", "value": "other text", "datatype": "http://www.w3.org/2001/XMLSchema#string"}
            }
        ]
    },
    set()
)
```

### Filling timestamp gaps

By default, the result only contains timestamps where the queried entities actually changed. `run_agnostic_query()` accepts an optional `include_all_timestamps` parameter. When `True`, the result dictionary includes entries for all provenance timestamps in the dataset (including those where only other entities changed), carrying forward the last known result for each gap:

```python
results, _ = query.run_agnostic_query(include_all_timestamps=True)
```

## Cross-version structured query

A cross-version structured query runs a SPARQL query on all the dataset's versions.

Instantiate `VersionQuery` without the `on_time` parameter:

```python
from time_agnostic_library.agnostic_query import VersionQuery

query = VersionQuery(query=QUERY_STRING, config_path=CONFIG_PATH)
results, _ = query.run_agnostic_query()
```

The output follows the same format, with one entry per version. The second element is always an empty set for cross-version queries:

```python
(
    {
        TIME_1: [
            {
                "var1": {"type": "uri", "value": "http://example.com/res/1"},
                "var2": {"type": "literal", "value": "some text"}
            }
        ],
        TIME_2: [
            {
                "var1": {"type": "uri", "value": "http://example.com/res/2"},
                "var2": {"type": "literal", "value": "other text"}
            }
        ]
    },
    set()
)
```
