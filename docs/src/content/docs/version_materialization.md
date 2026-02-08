---
title: Version materialization
description: Retrieve entity state at a specific time or get complete entity history
---

Version materialization returns an entity's state at a given time. The time can be an interval, an instant, a before, or an after.

## Get state at time

Create an `AgnosticEntity` instance with the entity URI and a configuration dictionary, then call `get_state_at_time()`:

```python
import json
from time_agnostic_library.agnostic_entity import AgnosticEntity

with open(CONFIG_PATH) as f:
    config = json.load(f)

entity = AgnosticEntity(res=RES_URI, config=config)
entity.get_state_at_time(time=(START, END), include_prov_metadata=True)
```

The time is a tuple `(START, END)`. If one value is `None`, only the other is considered. Any standard datetime format is accepted.

The output is a tuple of three elements:

```python
(
    {
        RES_URI: {
            TIME_1: ENTITY_GRAPH_AT_TIME_1,
            TIME_2: ENTITY_GRAPH_AT_TIME_2
        }
    },
    {
        RES_URI: {
            SNAPSHOT_URI_AT_TIME_1: {
                'generatedAtTime': TIME_1,
                'wasAttributedTo': ATTRIBUTION,
                'hadPrimarySource': PRIMARY_SOURCE
            },
            SNAPSHOT_URI_AT_TIME_2: {
                'generatedAtTime': TIME_2,
                'wasAttributedTo': ATTRIBUTION,
                'hadPrimarySource': PRIMARY_SOURCE
            }
        }
    },
    {
        RES_URI: {
            OTHER_SNAPSHOT_URI_1: {
                'generatedAtTime': GENERATION_TIME,
                'wasAttributedTo': ATTRIBUTION,
                'hadPrimarySource': PRIMARY_SOURCE
            },
            OTHER_SNAPSHOT_URI_2: {
                'generatedAtTime': GENERATION_TIME,
                'wasAttributedTo': ATTRIBUTION,
                'hadPrimarySource': PRIMARY_SOURCE
            }
        }
    }
)
```

1. A dictionary mapping entity URIs to their graphs at timestamps within the specified interval
2. Snapshots metadata for the returned states if `include_prov_metadata` is `True`, empty dictionary if `False`
3. Other snapshots' provenance metadata if `include_prov_metadata` is `True`, empty dictionary if `False`

## Get full history

To retrieve the complete history of an entity, use `get_history()`:

```python
entity = AgnosticEntity(res=RES_URI, config=config)
entity.get_history(include_prov_metadata=True)
```

The output is a two-element tuple:

```python
(
    {
        RES_URI: {
            TIME_1: ENTITY_GRAPH_AT_TIME_1,
            TIME_2: ENTITY_GRAPH_AT_TIME_2
        }
    },
    {
        RES_URI: {
            SNAPSHOT_URI_AT_TIME_1: {
                'generatedAtTime': GENERATION_TIME,
                'wasAttributedTo': ATTRIBUTION,
                'hadPrimarySource': PRIMARY_SOURCE
            },
            SNAPSHOT_URI_AT_TIME_2: {
                'generatedAtTime': GENERATION_TIME,
                'wasAttributedTo': ATTRIBUTION,
                'hadPrimarySource': PRIMARY_SOURCE
            }
        }
    }
)
```

1. A dictionary containing all versions of the entity, keyed by entity URI
2. All provenance metadata linked to that entity if `include_prov_metadata` is `True`, `None` if `False`

## Related entities

The history of an entity and all related entities can be obtained by setting the `include_*` parameters:

```python
entity = AgnosticEntity(
    res=RES_URI,
    config=config,
    include_related_objects=True,
    include_merged_entities=True,
    include_reverse_relations=False
)
entity.get_history(include_prov_metadata=True)
```
