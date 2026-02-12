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

The time is a tuple `(START, END)`. If one value is `None`, only the other is considered. Dates must be in ISO 8601 format (e.g., `2023-01-01`, `2023-01-01T00:00:00+00:00`).

The output is a tuple of three elements:

```python
(
    {
        TIME_1: ENTITY_QUAD_SET_AT_TIME_1,
        TIME_2: ENTITY_QUAD_SET_AT_TIME_2
    },
    {
        SNAPSHOT_URI_AT_TIME_1: {
            'generatedAtTime': TIME_1,
            'invalidatedAtTime': INVALIDATION_TIME,
            'wasAttributedTo': ATTRIBUTION,
            'hasUpdateQuery': UPDATE_QUERY,
            'hadPrimarySource': PRIMARY_SOURCE,
            'description': DESCRIPTION
        }
    },
    {
        OTHER_SNAPSHOT_URI_1: {
            'generatedAtTime': GENERATION_TIME,
            'invalidatedAtTime': INVALIDATION_TIME,
            'wasAttributedTo': ATTRIBUTION,
            'hasUpdateQuery': UPDATE_QUERY,
            'hadPrimarySource': PRIMARY_SOURCE,
            'description': DESCRIPTION
        }
    }
)
```

1. A dictionary mapping timestamps to entity quad sets within the specified interval. Each quad set is a `set[tuple[str, ...]]` where each tuple contains N3-encoded RDF terms (e.g., `("<http://example.com/s>", "<http://example.com/p>", "\"value\"")`)
2. Snapshots metadata for the returned states if `include_prov_metadata` is `True`, empty dictionary if `False`
3. Other snapshots' provenance metadata if `include_prov_metadata` is `True`, empty dictionary if `False`

When `include_related_objects`, `include_merged_entities`, or `include_reverse_relations` are enabled, the output tuple has the same structure but each element gains an extra nesting level keyed by entity URI, since the result can contain multiple entities:

```python
(
    {
        RES_URI_1: {
            TIME_1: ENTITY_QUAD_SET_AT_TIME_1,
            TIME_2: ENTITY_QUAD_SET_AT_TIME_2
        },
        RES_URI_2: {
            TIME_1: ENTITY_QUAD_SET_AT_TIME_1
        }
    },
    {
        RES_URI_1: {
            SNAPSHOT_URI: { ... }
        }
    },
    {
        RES_URI_1: {
            OTHER_SNAPSHOT_URI: { ... }
        }
    }
)
```

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
            TIME_1: ENTITY_QUAD_SET_AT_TIME_1,
            TIME_2: ENTITY_QUAD_SET_AT_TIME_2
        }
    },
    {
        RES_URI: {
            SNAPSHOT_URI_AT_TIME_1: {
                'generatedAtTime': GENERATION_TIME,
                'invalidatedAtTime': INVALIDATION_TIME,
                'wasAttributedTo': ATTRIBUTION,
                'hadPrimarySource': PRIMARY_SOURCE,
                'description': DESCRIPTION,
                'hasUpdateQuery': UPDATE_QUERY,
                'wasDerivedFrom': [DERIVED_SNAPSHOT_URI_1, ...]
            }
        }
    }
)
```

1. A dictionary containing all versions of the entity, keyed by entity URI. Each version is a `set[tuple[str, ...]]` of N3-encoded quad tuples
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
