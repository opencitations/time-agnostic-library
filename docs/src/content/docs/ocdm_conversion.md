---
title: OCDM conversion
description: Convert RDF versioned data into OCDM-compliant format using independent copies or change-based strategies
---

The `OCDMConverter` class converts versioned RDF data into OCDM-compliant N-Quads files (dataset + provenance), ready for querying with the library. It supports two ingestion strategies:

- **Independent copies (IC)**: each version is a complete snapshot. The converter computes diffs between consecutive versions and generates provenance metadata.
- **Change-based (CB)**: an initial snapshot plus a sequence of delta files (additions and deletions). The converter applies deltas incrementally and generates provenance metadata.

## Independent copies

Use this strategy when you have complete snapshots for each version.

```python
from datetime import datetime, timedelta, timezone
from pathlib import Path

from time_agnostic_library.ocdm_converter import OCDMConverter

converter = OCDMConverter(
    data_graph_uri="http://example.org/data/",
    agent_uri="http://example.org/agent/1",
)

ic_files = [Path(f"snapshots/v{i}.nt") for i in range(10)]
timestamps = [
    datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=i)
    for i in range(10)
]

converter.convert_from_ic(
    ic_files=ic_files,
    timestamps=timestamps,
    dataset_output=Path("output/dataset.nq"),
    provenance_output=Path("output/provenance.nq"),
)
```

Parameters:
- `ic_files`: list of N-Triples file paths (`.nt` or `.nt.gz`), sorted by version
- `timestamps`: list of `datetime` objects, one per version
- `dataset_output`: path for the generated dataset N-Quads file (latest state)
- `provenance_output`: path for the generated provenance N-Quads file (snapshot history)

## Change-based

Use this strategy when you have an initial snapshot and a sequence of delta files describing additions and deletions between consecutive versions.

```python
from datetime import datetime, timedelta, timezone
from pathlib import Path

from time_agnostic_library.ocdm_converter import OCDMConverter

converter = OCDMConverter(
    data_graph_uri="http://example.org/data/",
    agent_uri="http://example.org/agent/1",
)

changesets = [
    (Path("deltas/added_0-1.nt"), Path("deltas/deleted_0-1.nt")),
    (Path("deltas/added_1-2.nt"), Path("deltas/deleted_1-2.nt")),
]
timestamps = [
    datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=i)
    for i in range(3)
]

converter.convert_from_cb(
    initial_snapshot=Path("snapshots/v0.nt"),
    changesets=changesets,
    timestamps=timestamps,
    dataset_output=Path("output/dataset.nq"),
    provenance_output=Path("output/provenance.nq"),
)
```

Parameters:
- `initial_snapshot`: path to the N-Triples file for version 0
- `changesets`: list of `(added_file, deleted_file)` tuples, sorted by version. Each tuple contains paths to N-Triples files with added and deleted triples for that version transition
- `timestamps`: list of `datetime` objects. `timestamps[0]` is for the initial snapshot, `timestamps[i+1]` for changeset `i`
- `dataset_output`: path for the generated dataset N-Quads file
- `provenance_output`: path for the generated provenance N-Quads file

## Object normalization

Some SPARQL backends require specific datatype representations. The `object_normalizer` parameter allows custom transformations on triple objects during parsing:

```python
XSD = "http://www.w3.org/2001/XMLSchema#"

def normalize_for_qlever(obj: str) -> str:
    if obj.endswith(f"^^<{XSD}integer>"):
        return obj.replace(f"^^<{XSD}integer>", f"^^<{XSD}int>")
    return obj

converter = OCDMConverter(
    data_graph_uri="http://example.org/data/",
    agent_uri="http://example.org/agent/1",
    object_normalizer=normalize_for_qlever,
)
```

## Output format

Both strategies produce two N-Quads files:

**dataset.nq** contains the latest state of all entities in the specified data graph:

```
<http://example.org/entity1> <http://xmlns.com/foaf/0.1/name> "Alice" <http://example.org/data/> .
```

**provenance.nq** contains OCDM-compliant provenance metadata with snapshots, timestamps, and SPARQL UPDATE queries in entity-specific provenance graphs:

```
<http://example.org/entity1/prov/se/1> <http://www.w3.org/ns/prov#specializationOf> <http://example.org/entity1> <http://example.org/entity1/prov/> .
<http://example.org/entity1/prov/se/1> <http://www.w3.org/ns/prov#generatedAtTime> "2024-01-01T00:00:00+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> <http://example.org/entity1/prov/> .
```