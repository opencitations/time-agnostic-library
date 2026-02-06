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

The time is a tuple `(START, END)`. If one value is `None`, only the other is considered. Any standard datetime format is accepted.

The output is a tuple of two elements. The first is a dictionary where keys are timestamps and values are lists of bindings in the [W3C SPARQL JSON results format](https://www.w3.org/TR/sparql11-results-json/). The second is a set of timestamps from other relevant snapshots:

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
    {OTHER_TIMESTAMP_1, OTHER_TIMESTAMP_2}
)
```

## Cross-version structured query

A cross-version structured query runs a SPARQL query on all the dataset's versions.

Instantiate `VersionQuery` without the `on_time` parameter:

```python
from time_agnostic_library.agnostic_query import VersionQuery

query = VersionQuery(query=QUERY_STRING, config_path=CONFIG_PATH)
results, other_timestamps = query.run_agnostic_query()
```

The output follows the same format, with one entry per version:

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
    {OTHER_TIMESTAMP_1, OTHER_TIMESTAMP_2}
)
```
