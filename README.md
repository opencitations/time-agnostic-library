[<img src="https://img.shields.io/badge/powered%20by-OpenCitations-%239931FC?labelColor=2D22DE" />](http://opencitations.net)
[![run_tests](https://github.com/opencitations/time-agnostic-library/actions/workflows/run_tests.yml/badge.svg)](https://github.com/opencitations/time-agnostic-library/actions/workflows/run_tests.yml)
![Coverage](https://raw.githubusercontent.com/opencitations/time-agnostic-library/main/tests/coverage/coverage.svg)
![PyPI](https://img.shields.io/pypi/pyversions/time-agnostic-library)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/opencitations/time-agnostic-library)

# time-agnostic-library

**time-agnostic-library** is a Python &ge;3.7 library that allows performing time-travel queries on RDF datasets compliant with the [OCDM v2.0.1](https://figshare.com/articles/Metadata_for_the_OpenCitations_Corpus/3443876) provenance specification.

The package was tested on **Blazegraph**, **GraphDB**, **Apache Jena Fuseki**, and **OpenLink Virtuoso**, and it is fully compatible with these triplestores. 

Documentation can be found here: [https://time-agnostic-library.readthedocs.io](https://time-agnostic-library.readthedocs.io).

## Table of Contents

- [User's guide](#users-guide)
  * [Version materialization](#version-materialization)
  * [Single-version structured query](#single-version-structured-query)
  * [Cross-version structured query](#cross-version-structured-query)
  * [Single-delta structured query](#single-delta-structured-query)
  * [Cross-delta structured query](#cross-delta-structured-query)
  * [Configuration file](#configuration-file)
- [Developer's guide](#developers-guide)
  * [First steps](#first-steps)
  * [How to run the tests](#how-to-run-the-tests)
  * [How to build the documentation](#how-to-build-the-documentation)
  * [How to benchmark](#how-to-benchmark)

## User's guide

This package can be installed with **pip**:

``` bash
  pip install time-agnostic-library
```

The library allows five types of queries: versions materializations, single-version structured queries, cross-version structured queries, single-delta structured queries and cross-delta structured queries.

### Version materialization

Obtaining a version materialization means returning an entity version at a given time. Time can be an interval, an instant, a before or an after.

To do so, create an instance of the **AgnosticEntity** class, passing as an argument the entity URI (RES_URI) and the configuration file path (CONFIG_PATH). For more information about the configuration file, see [Configuration file](#configuration-file). Finally, run the **get_state_at_time method**, passing the time of interest as an argument and, if provenance metadata are needed, True to the **include_prov_metadata** field. 

The specified time is a tuple, in the form (START, END). If one of the two values is None, only the other is considered. The time can be specified using any existing standard.

``` python
  agnostic_entity = AgnosticEntity(res=RES_URI, config_path=CONFIG_PATH)
  agnostic_entity.get_state_at_time(time=(START, END), include_prov_metadata=True)
```

The output is always a tuple of three elements: the first is a dictionary that associates graphs and timestamps within the specified interval; the second contains the snapshots metadata of which the states has been returned, the third is a dictionary including the other snapshots' provenance metadata if **include_prov_metadata** is True, None if False.

``` python
  (
      {
          TIME_1: ENTITY_GRAPH_AT_TIME_1, 
          TIME_2: ENTITY_GRAPH_AT_TIME_2
      },
      {
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
      }, 
      {
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
  )
```

On the other hand, if the whole entity's history is required, use the **get_history** method.

``` python
  agnostic_entity = AgnosticEntity(res=RES_URI, config_path=CONFIG_PATH)
  agnostic_entity.get_history(include_prov_metadata=True)
```

In this case, the output is always a two-element tuple. The first is a dictionary containing all the versions of a given resource. The second is a dictionary containing all the provenance metadata linked to that resource if **include_prov_metadata** is True, None if False.

``` python
  (
    {
       RES_URI: {
            TIME_1: ENTITY_GRAPH_AT_TIME_1, 
            TIME_2: ENTITY_GRAPH_AT_TIME_2
        }
    },
    {
      RES_URI: {
        SNAPSHOT_URI_AT_TIME_1': {
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
  )
```

Finally, the history of an entity and all related entities, along with their provenance metadata, can be obtained by switching to True the **related_entities_history** argument.

``` python
  agnostic_entity = AgnosticEntity(res=RES_URI, related_entities_history=True, config_path=CONFIG_PATH)
  agnostic_entity.get_history(include_prov_metadata=True)
```

### Single-version structured query

Performing a single-version structured query means running a SPARQL query on a specified version. A version can be a time interval, an instant, a before or an after.

To obtain this result, instantiate the **VersionQuery** class, passing as an argument the SPARQL query string (QUERY_STRING), the time of interest and the configuration file's path (CONFIG_PATH). Finally, execute the **run_agnostic_query** method.

The specified time is a tuple, in the form (START, END). If one of the two values is None, only the other is considered. The time can be specified using any existing standard.

For more information about the configuration file, see [Configuration file](#configuration-file). 

**Please note**: only SELECT queries are allowed. 

``` python
  agnostic_query = VersionQuery(query=QUERY_STRING, on_time=(START, END), config_path=CONFIG_PATH)
  agnostic_query.run_agnostic_query()
```

The output is a dictionary in which the keys are the snapshots relevant to that query. The values correspond to sets of tuples containing the query results at the time specified by the key. The positional value of the elements in the tuples is equivalent to the variables indicated in the query.

``` python
  {
    TIME: {
      (VALUE_1_OF_VARIABLE_1, VALUE_1_OF_VARIABLE_2, VALUE_1_OF_VARIABLE_N),
      (VALUE_2_OF_VARIABLE_1, VALUE_2_OF_VARIABLE_2, VALUE_2_OF_VARIABLE_N),
      (VALUE_N_OF_VARIABLE_1, VALUE_N_OF_VARIABLE_2, VALUE_N_OF_VARIABLE_N)
    }
  }
```

### Cross-version structured query

Performing a cross-version structured query means running a SPARQL query on all the dataset's versions.

To obtain this result, instantiate the **VersionQuery** class, passing as an argument the SPARQL query string (QUERY_STRING) and the configuration file's path (CONFIG_PATH). Finally, execute **the run_agnostic_query** method.

For more information about the configuration file, see [Configuration file](#configuration-file). 

**Please note**: only SELECT queries are allowed. 

``` python
  agnostic_query = VersionQuery(query=QUERY_STRING, config_path=CONFIG_PATH)
  agnostic_query.run_agnostic_query()
```

The output is a dictionary in which the keys are the snapshots relevant to that query. The values correspond to sets of tuples containing the query results at the time specified by the key. The positional value of the elements in the tuples is equivalent to the variables indicated in the query.

``` python
  {
    TIME_1: {
      (VALUE_1_OF_VARIABLE_1, VALUE_1_OF_VARIABLE_2, VALUE_1_OF_VARIABLE_N),
      (VALUE_2_OF_VARIABLE_1, VALUE_2_OF_VARIABLE_2, VALUE_2_OF_VARIABLE_N),
      (VALUE_N_OF_VARIABLE_1, VALUE_N_OF_VARIABLE_2, VALUE_N_OF_VARIABLE_N)
    },
    TIME_2: {
      (VALUE_1_OF_VARIABLE_1, VALUE_1_OF_VARIABLE_2, VALUE_1_OF_VARIABLE_N),
      (VALUE_2_OF_VARIABLE_1, VALUE_2_OF_VARIABLE_2, VALUE_2_OF_VARIABLE_N),
      (VALUE_N_OF_VARIABLE_1, VALUE_N_OF_VARIABLE_2, VALUE_N_OF_VARIABLE_N)
    },
    TIME_N: {
      (VALUE_1_OF_VARIABLE_1, VALUE_1_OF_VARIABLE_2, VALUE_1_OF_VARIABLE_N),
      (VALUE_2_OF_VARIABLE_1, VALUE_2_OF_VARIABLE_2, VALUE_2_OF_VARIABLE_N),
      (VALUE_N_OF_VARIABLE_1, VALUE_N_OF_VARIABLE_2, VALUE_N_OF_VARIABLE_N)
    }
  }
```

### Single-delta structured query

Performing a single-delta query means that a structured queries must be satisfied on a specific change instance of the dataset. This information demand particularly focuses on the differences between two versions, which are typically but not always consecutive.

To obtain this result, instantiate the **DeltaQuery** class, passing as an argument the SPARQL query string, the time of interest, a set of properties, and the configuration file's path. Finally, execute the **run_agnostic_query** method.

The query string (QUERY_STRING) is useful to identify the entities whose change you want to investigate.

The specified time is a tuple, in the form (START, END). If one of the two values is None, only the other is considered. The time can be specified using any existing standard.

The set of properties (PROPERTIES_SET) narrows the field to those entities where the properties specified in the set have changed. If no property was indicated, any changes are considered.

For more information about the configuration file (CONFIG_PATH), see [Configuration file](#configuration-file). 

**Please note**: only SELECT queries are allowed. 

``` python
  agnostic_entity = DeltaQuery(query=QUERY_STRING, on_time=(START, END), changed_properties=PROPERTIES_SET, config_path=CONFIG_PATH)
  agnostic_entity.run_agnostic_query()
```

The output is a dictionary that reports the modified entities, when they were created, modified, and deleted. Moreover, changes are reported as SPARQL UPDATE queries.

``` python
  {
      RES_URI_1: {
          "created": TIMESTAMP_CREATION,
          "modified": {
              TIMESTAMP_1: UPDATE_QUERY_1,
              TIMESTAMP_2: UPDATE_QUERY_2,
              TIMESTAMP_N: UPDATE_QUERY_N
          },
          "deleted": TIMESTAMP_DELETION
      },
      RES_URI_2: {
          "created": TIMESTAMP_CREATION,
          "modified": {
              TIMESTAMP_1: UPDATE_QUERY_1,
              TIMESTAMP_2: UPDATE_QUERY_2,
              TIMESTAMP_N: UPDATE_QUERY_N
          },
          "deleted": TIMESTAMP_DELETION
      },
      RES_URI_N: {
          "created": TIMESTAMP_CREATION,
          "modified": {
              TIMESTAMP_1: UPDATE_QUERY_1,
              TIMESTAMP_2: UPDATE_QUERY_2,
              TIMESTAMP_N: UPDATE_QUERY_N
          },
          "deleted": TIMESTAMP_DELETION
      },		
  }    
```

### Cross-delta structured query

Performing a cross-delta query means that a structured queries must be satisfied across the differences of the dataset, thus allowing for fully fledged evolution studies.

To obtain this result, instantiate the **DeltaQuery** class, passing as an argument the SPARQL query string, a set of properties, and the configuration file's path. Finally, execute the **run_agnostic_query** method.

The query string (QUERY_STRING) is useful to identify the entities whose change you want to investigate.

The set of properties (PROPERTIES_SET) narrows the field to those entities where the properties specified in the set have changed. If no property was indicated, any changes are considered.

For more information about the configuration file (CONFIG_PATH), see [Configuration file](#configuration-file). 

**Please note**: only SELECT queries are allowed. 

``` python
  agnostic_entity = DeltaQuery(query=QUERY_STRING, changed_properties=PROPERTIES_SET, config_path=CONFIG_PATH)
  agnostic_entity.run_agnostic_query()
```

The output is a dictionary that reports the modified entities, when they were created, modified, and deleted. Changes are reported as SPARQL UPDATE queries. If the entity was not created or deleted within the indicated range, the "created" or "deleted" value is None. On the other hand, if the entity does not exist within the input interval, the "modified" value is an empty dictionary.

``` python
  {
      RES_URI_1: {
          "created": TIMESTAMP_CREATION,
          "modified": {
              TIMESTAMP_1: UPDATE_QUERY_1,
              TIMESTAMP_2: UPDATE_QUERY_2,
              TIMESTAMP_N: UPDATE_QUERY_N
          },
          "deleted": TIMESTAMP_DELETION
      },
      RES_URI_2: {
          "created": TIMESTAMP_CREATION,
          "modified": {
              TIMESTAMP_1: UPDATE_QUERY_1,
              TIMESTAMP_2: UPDATE_QUERY_2,
              TIMESTAMP_N: UPDATE_QUERY_N
          },
          "deleted": TIMESTAMP_DELETION
      },
      RES_URI_N: {
          "created": TIMESTAMP_CREATION,
          "modified": {
              TIMESTAMP_1: UPDATE_QUERY_1,
              TIMESTAMP_2: UPDATE_QUERY_2,
              TIMESTAMP_N: UPDATE_QUERY_N
          },
          "deleted": TIMESTAMP_DELETION
      },		
  }    
```

### Configuration file

The configuration file is mainly used to indicate to the library where to search for data and provenance. In addition, some optional values can be set to make executions faster and more efficient.

- **dataset** (required)
  - **triplestore_urls**: Specify a list of triplestore URLs containing data.  
  - **file_paths**: Specify a list of paths of files containing data.   
- **provenance** (required)
  - **triplestore_urls**: Specify a list of triplestore URLs containing provenance metadata.    
  - **file_paths** Specify a list of paths of files containing provenance metadata.      
- **blazegraph_full_text_search**, **fuseki_full_text_search**, **virtuoso_full_text_search** (optional): Specify an affirmative Boolean value if Blazegraph, Fuseki or Virtuoso was used as a triplestore, and a textual index was built to speed up queries. The allowed values are "true", "1", 1, "t", "y", "yes", "ok", or "false", "0", 0, "n", "f", "no".
- **graphdb_connector_name** (optional): Specify the name of the Lucene connector if GraphDB was used as a triplestore and a textual index was built to speed up queries. For more information, see [https://graphdb.ontotext.com/documentation/free/general-full-text-search-with-connectors.html](https://graphdb.ontotext.com/documentation/free/general-full-text-search-with-connectors.html).
- **cache_triplestore_url** (optional): Specifies the triplestore URL to use as a cache to make queries faster. If your triplestore uses different endpoints for reading and writing (e.g. GraphDB), specify the endpoint for reading in the "endpoint" field and the endpoint for writing in the "update_endpoint" field. If there is only one endpoint (e.g. Blazegraph), specify it in both fields.

``` json
  {
      "dataset": {
          "triplestore_urls": ["TRIPLESTORE_URL_1", "TRIPLESTORE_URL_2", "TRIPLESTORE_URL_N"],
          "file_paths": ["PATH_1", "PATH_2", "PATH_N"]
      },
      "provenance": {
          "triplestore_urls": ["TRIPLESTORE_URL_1", "TRIPLESTORE_URL_2", "TRIPLESTORE_URL_N"],
          "file_paths": ["PATH_1", "PATH_2", "PATH_N"]
      },
      "blazegraph_full_text_search": "no",
      "fuseki_full_text_search": "no",
      "virtuoso_full_text_search": "no",
      "graphdb_connector_name": "CONNECTOR_NAME",
      "cache_triplestore_url": {
        "endpoint": "READ_ENDPOINT",
        "update_endpoint": "UPDATE_ENDPOINT"
    }
  }
```

## Developer's guide

### First steps
  1. Install Poetry:
``` bash
    pip install poetry
```
  2. Clone this repository:
``` bash
    git clone https://github.com/opencitations/time-agnostic-library
    cd ./time-agnostic-library
```
  3. Install all the dependencies:
``` bash
    poetry install
```
  4. Build the package (_output dir:_ `dist`):
``` bash
    poetry build
```
  5. Globally install the package:
``` bash
    pip install ./dist/time-agnostic-library-<VERSION>.tar.gz
```
  6. If everything went the right way, than you should be able to use `time_agnostic_library` in your Python modules as follows:
``` python
    from time_agnostic_library.agnostic_entity import AgnosticEntity
    from time_agnostic_library.agnostic_query import AgnosticQuery
    # ...
```

### How to run the tests

 1. Make sure that Java is installed on your computer.
 2. Simply launch the following command from the root folder:

``` bash
  poetry run test
```

### How to build the documentation

  1. Move inside the `docs` folder:
``` bash
  cd docs
```
  2. Use the Makefile provided to build the docs:
      + _on Windows_
        ```
          ./make.bat html
        ```
      + _on Linux and MacOs_
        ```
          make html
        ```
  3. Open the `_build/html/index.html` file with a web browser of your choice!

### How to benchmark

Two benchmarks were designed, one on the execution times and the other on the RAM. Moreover, all benchmarks are performed on four different triplestores: Blazegraph, GraphDB Free Edition, Apache Jena Fuseki, and OpenLink Virtuoso.

The dataset used for the benchmarks contains bibliographical information about scholarly works in the journal Scientometrics only if the DOI is known. The data was extracted via Crossref. It is a temporal dataset in which provenance information and change-tracking have been managed by adopting the OpenCitations Data Model. Moreover, the dataset contains information on all the cited academic works. Journals, bibliographic resources, and authors always appear unambiguously, without duplicates. Finally, heuristics have been applied to recover the DOI of the cited works in case Crossref did not provide such information.

In order to run the benchmark, do the following steps:
1. Download the dataset from [10.5281/zenodo.7105258](https://doi.org/10.5281/zenodo.7105258). 
2. Extract `reproduce_results.zip`. 
    + _on Windows_
      
      Execute `run_banchmarks.bat`
    + _on Linux and MacOs_
      
      Execute `run_banchmarks.sh`
3. As the results become available, they can be read by opening `json_to_table.html` via a local server.

In the event that the execution should stop due to unforeseen causes, it will resume from where it was interrupted.


