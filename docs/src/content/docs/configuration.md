---
# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

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
- **qlever_full_text_search** (optional): disabled by default; set to `"yes"` after preparing the URI index described below.
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
    "qlever_full_text_search": "no",
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

When `fuseki_full_text_search` is on, the Lucene index over `hasUpdateQuery`
must split text on spaces, because the library looks up an IRI as a whole
token. The default analyzer breaks an IRI into words, so a namespace root such
as `<http://www.w3.org/>` matches every IRI below it.

```turtle
@prefix text: <http://jena.apache.org/text#> .

:index a text:TextIndexLucene ;
    text:directory <file:/path/to/lucene> ;
    text:entityMap :entity_map ;
    text:analyzer [
        a text:ConfigurableAnalyzer ;
        text:tokenizer text:WhitespaceTokenizer
    ] .

:entity_map a text:EntityMap ;
    text:entityField "uri" ;
    text:defaultField "updateQuery" ;
    text:map (
        [
            text:field "updateQuery" ;
            text:predicate <https://w3id.org/oc/ontology/hasUpdateQuery>
        ]
    ) .
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

### QLever

A QLever index built directly from the dataset and provenance lacks the URI associations required by this search. The setup script extracts URIs from the provenance update queries and links them to their snapshots before indexing.

Install the QLever extra and make sure Docker is running before starting the setup:

```sh
uv add "time-agnostic-library[qlever]"
```

From this repository, `uv sync --dev` also installs the required tools. Run the setup with your current dataset and OCDM provenance files:

```sh
uv run python -m time_agnostic_library.qlever_setup \
    --dataset dataset.nq \
    --provenance provenance.nq \
    --output-dir qlever_index \
    --port 7000
```

Both input options accept several N-Quads files, so you can pass separate shards without joining them yourself. The provenance must contain the snapshots and SPARQL updates that describe the dataset history.

The setup builds the QLever index through Docker, starts the server, and checks that its endpoint answers a query. Docker downloads the QLever image if it is missing. Once the server responds, the setup writes `qlever_index/config.json` with both endpoints and URI search enabled. Pass that file to `VersionQuery` or `DeltaQuery`:

```python
from time_agnostic_library.agnostic_query import VersionQuery

query = VersionQuery(
    "SELECT ?s WHERE { ?s <https://example.org/p> <https://example.org/o> }",
    config_path="qlever_index/config.json",
)
results = query.run_agnostic_query()
```

The server remains running after the command finishes. For the port in this example, stop its container with:

```sh
docker stop tal-qlever-7000
```

SPARQL endpoint example: `http://127.0.0.1:7001`

## Generating a configuration file programmatically

Use `generate_config_file()` from the support module:

```python
from time_agnostic_library.support import generate_config_file

generate_config_file(
    config_path="./config.json",
    dataset_urls=["http://127.0.0.1:9999/blazegraph/sparql"],
    provenance_urls=["http://127.0.0.1:19999/blazegraph/sparql"],
)
```
