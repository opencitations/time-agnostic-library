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

## Tested triplestores

The library has been tested on the following SPARQL triplestores. Each one has specific configuration requirements to be aware of.

### Blazegraph

Blazegraph must run in **quads mode**, which [does not support inference](https://github.com/blazegraph/database/wiki/InferenceAndTruthMaintenance). The namespace must be created with `quads=true`, `truthMaintenance=false`, and `axiomsClass=NoAxioms` -- Blazegraph refuses to create a quads namespace without these three settings.

SPARQL endpoint example: `http://127.0.0.1:9999/blazegraph/namespace/kb/sparql`

### Apache Jena Fuseki

Fuseki must use a TDB2 dataset configured with `tdb2:unionDefaultGraph true`. The library issues some internal queries (particularly for provenance data) without explicit `GRAPH` clauses. Without the union default graph setting, these queries return empty results because Fuseki's default graph does not include data stored in named graphs.

This setting must be applied via a Turtle assembler configuration when creating the dataset, not through the simpler `dbType=tdb2` REST parameter:

```turtle
@prefix tdb2: <http://jena.apache.org/2016/tdb#> .

:dataset a tdb2:DatasetTDB2 ;
    tdb2:location "/path/to/database" ;
    tdb2:unionDefaultGraph true .
```

SPARQL endpoint example: `http://127.0.0.1:3030/dataset`

### GraphDB Free Edition

GraphDB works with the library out of the box.

GraphDB uses separate endpoints for queries and updates: queries go to `/repositories/{name}`, while SPARQL UPDATE operations go to `/repositories/{name}/statements`. The library only performs read operations, so only the query endpoint is needed in the configuration.

SPARQL endpoint example: `http://127.0.0.1:7200/repositories/myrepo`

### OpenLink Virtuoso

Virtuoso works with the library out of the box. The SPARQL endpoint must have read permissions on all graphs. If you also need to load data through the SPARQL endpoint, grant update permissions with:

```sql
DB.DBA.RDF_DEFAULT_USER_PERMS_SET('nobody', 7);
DB.DBA.USER_GRANT_ROLE('SPARQL', 'SPARQL_UPDATE');
```

SPARQL endpoint example: `http://127.0.0.1:8890/sparql`

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
