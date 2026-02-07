---
title: Configuration
description: Configuration file format and options for time-agnostic-library
---

The configuration tells the library where to find data and provenance. `AgnosticEntity` accepts a `config` dictionary directly. `VersionQuery` and `DeltaQuery` accept either a `config_path` (path to a JSON file) or a `config_dict` (dictionary).

## Configuration options

- **dataset** (required)
  - **triplestore_urls**: list of triplestore URLs containing data
  - **file_paths**: list of paths to files containing data
  - **is_quadstore**: whether the dataset store is a quadstore
- **provenance** (required)
  - **triplestore_urls**: list of triplestore URLs containing provenance metadata
  - **file_paths**: list of paths to files containing provenance metadata
  - **is_quadstore**: whether the provenance store is a quadstore
- **blazegraph_full_text_search** (optional): set to an affirmative value if Blazegraph was used and a textual index was built. Allowed values: `"true"`, `"1"`, `1`, `"t"`, `"y"`, `"yes"`, `"ok"`, or `"false"`, `"0"`, `0`, `"n"`, `"f"`, `"no"`
- **fuseki_full_text_search** (optional): same as above, for Apache Jena Fuseki
- **virtuoso_full_text_search** (optional): same as above, for OpenLink Virtuoso
- **graphdb_connector_name** (optional): name of the Lucene connector if GraphDB was used. See [GraphDB full-text search documentation](https://graphdb.ontotext.com/documentation/free/general-full-text-search-with-connectors.html)

## Example configuration file

```json
{
    "dataset": {
        "triplestore_urls": ["TRIPLESTORE_URL_1", "TRIPLESTORE_URL_2"],
        "file_paths": ["PATH_1", "PATH_2"],
        "is_quadstore": true
    },
    "provenance": {
        "triplestore_urls": ["TRIPLESTORE_URL_1", "TRIPLESTORE_URL_2"],
        "file_paths": ["PATH_1", "PATH_2"],
        "is_quadstore": true
    },
    "blazegraph_full_text_search": "no",
    "fuseki_full_text_search": "no",
    "virtuoso_full_text_search": "no",
    "graphdb_connector_name": "CONNECTOR_NAME"
}
```

## Generating a configuration file programmatically

Use `generate_config_file()` from the support module:

```python
from time_agnostic_library.support import generate_config_file

generate_config_file(
    config_path="./config.json",
    dataset_urls=["http://127.0.0.1:9999/blazegraph/sparql"],
    provenance_urls=["http://127.0.0.1:19999/blazegraph/sparql"]
)
```
