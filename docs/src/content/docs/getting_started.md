---
title: Getting started
description: Installation and quick start guide for time-agnostic-library
---

## Installation

```bash
pip install time-agnostic-library
```

## Requirements

- Python >= 3.10
- A SPARQL triplestore containing RDF data with [OCDM v2.0.1](https://figshare.com/articles/Metadata_for_the_OpenCitations_Corpus/3443876) provenance metadata

## Quick example

```python
import json
from time_agnostic_library.agnostic_entity import AgnosticEntity

with open("./config.json") as f:
    config = json.load(f)

entity = AgnosticEntity(res="https://example.com/br/1", config=config)
history, prov_metadata = entity.get_history(include_prov_metadata=True)
```

The library provides five types of queries:

- [Version materialization](/time-agnostic-library/version_materialization/) - retrieve entity state at a given time
- [Structured queries](/time-agnostic-library/structured_queries/) - run SPARQL queries on specific or all versions
- [Delta queries](/time-agnostic-library/delta_queries/) - track changes over time
