[<img src="https://img.shields.io/badge/powered%20by-OpenCitations-%239931FC?labelColor=2D22DE" />](http://opencitations.net)
[![run_tests](https://github.com/opencitations/time-agnostic-library/actions/workflows/run_tests.yml/badge.svg)](https://github.com/opencitations/time-agnostic-library/actions/workflows/run_tests.yml)
[![Coverage](https://byob.yarr.is/arcangelo7/badges/opencitations-time-agnostic-library-coverage-main)](https://opencitations.github.io/time-agnostic-library)
![PyPI](https://img.shields.io/pypi/pyversions/time-agnostic-library)
[![PyPI version](https://badge.fury.io/py/time-agnostic-library.svg)](https://badge.fury.io/py/time-agnostic-library)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/opencitations/time-agnostic-library)

# time-agnostic-library

**time-agnostic-library** is a Python >=3.10 library that allows performing time-travel queries on RDF datasets compliant with the [OCDM v2.0.1](https://figshare.com/articles/Metadata_for_the_OpenCitations_Corpus/3443876) provenance specification.

Tested on Blazegraph, GraphDB, Apache Jena Fuseki, and OpenLink Virtuoso.

Full documentation: [https://opencitations.github.io/time-agnostic-library](https://opencitations.github.io/time-agnostic-library)

## Quick start

```bash
pip install time-agnostic-library
```

```python
import json
from time_agnostic_library.agnostic_entity import AgnosticEntity

with open("./config.json") as f:
    config = json.load(f)

# Get the full history of an entity
entity = AgnosticEntity(res="https://example.com/br/1", config=config)
history, prov_metadata = entity.get_history(include_prov_metadata=True)

# Get an entity's state at a specific time
entity.get_state_at_time(time=("2023-01-01", "2023-12-31"))
```

```python
from time_agnostic_library.agnostic_query import VersionQuery, DeltaQuery

# Run a SPARQL query on a specific version
query = VersionQuery(query="SELECT ...", on_time=("2023-01-01", None), config_path="./config.json")
query.run_agnostic_query()

# Track changes across the entire history
delta = DeltaQuery(query="SELECT ...", config_path="./config.json")
delta.run_agnostic_query()
```

All date/time values must be in ISO 8601 format (e.g., `2023-01-01`, `2023-01-01T00:00:00+00:00`).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, testing, and release process.

## Changelog

All notable changes are documented in [CHANGELOG.md](CHANGELOG.md).
