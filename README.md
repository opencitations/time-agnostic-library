# time-agnostic-library

**time-agnostic-library** is a Python &ge;3.7 library that allows performing time-travel queries on RDF datasets compliant with the [OCDM v2.0.1](https://figshare.com/articles/Metadata_for_the_OpenCitations_Corpus/3443876) provenance specification.

Documentation can be found here: [https://time-agnostic-library.readthedocs.io](https://time-agnostic-library.readthedocs.io).

## Table of Contents

- [User's guide](#users-guide)
  * [Version materialization](#version-materialization)
  * [Single-version structured query](#single-version-structured-query)
  * [Cross-version structured query](#cross-version-structured-query)
- [Developer's guide](#developers-guide)
  * [How to build the documentation](#how-to-build-the-documentation)

## User's guide

This package can be installed with **pip**:

``` bash
  pip install time-agnostic-library
```

The library allows three types of queries: versions materializations, single-version structured queries and cross-version structured queries.

### Version materialization

Obtaining a version materialization means returning an entity version at a given time or at the closest past available time. 

To do so, create an instance of the **AgnosticEntity** class, passing as an argument the entity URI (RES_URI) and the configuration file path (CONFIG_PATH). For more information about the configuration file, see [Configuration file](#configuration-file). Finally, run the **get_state_at_time method**, passing the time of interest as an argument and, if provenance metadata are needed, True to the **include_prov_metadata** field. The specified time can be any time, not necessarily the exact time of a snapshot. In addition, it can be specified in any existing standard. 


``` python
  agnostic_entity = AgnosticEntity(res=RES_URI, config_path=CONFIG_PATH)
  agnostic_entity.get_state_at_time(time=TIME, include_prov_metadata=True)
```

The output is always a tuple of three elements: the first is the rdflib.Graph of the resource at that time (ENTITY_GRAPH_AT_TIME), the second is a dictionary containing the reconstructed snapshot's provenance metadata, the third is a dictionary containing the other snapshots' provenance metadata if include_prov_metadata is True, None if False.

``` python
  (
    ENTITY_GRAPH_AT_TIME, 
    {
        SNAPSHOT_URI_AT_TIME: {
            'http://www.w3.org/ns/prov#generatedAtTime': GENERATION_TIME, 
            'http://www.w3.org/ns/prov#wasAttributedTo': ATTRIBUTION, 
            'http://www.w3.org/ns/prov#hadPrimarySource': PRIMARY_SOURCE
        }
    }, 
    {
        OTHER_SNAPSHOT_URI_1: {
            'http://www.w3.org/ns/prov#generatedAtTime': GENERATION_TIME, 
            'http://www.w3.org/ns/prov#wasAttributedTo': ATTRIBUTION, 
            'http://www.w3.org/ns/prov#hadPrimarySource': PRIMARY_SOURCE
        }, 
        OTHER_SNAPSHOT_URI_2: {
            'http://www.w3.org/ns/prov#generatedAtTime': GENERATION_TIME, 
            'http://www.w3.org/ns/prov#wasAttributedTo': ATTRIBUTION, 
            'http://www.w3.org/ns/prov#hadPrimarySource': PRIMARY_SOURCE
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
            'http://www.w3.org/ns/prov#generatedAtTime': GENERATION_TIME, 
            'http://www.w3.org/ns/prov#wasAttributedTo': ATTRIBUTION, 
            'http://www.w3.org/ns/prov#hadPrimarySource': PRIMARY_SOURCE
        }, 
        SNAPSHOT_URI_AT_TIME_2: {
            'http://www.w3.org/ns/prov#generatedAtTime': GENERATION_TIME, 
            'http://www.w3.org/ns/prov#wasAttributedTo': ATTRIBUTION, 
            'http://www.w3.org/ns/prov#hadPrimarySource': PRIMARY_SOURCE
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

Performing a single-version structured query means running a SPARQL query on a specified version.

To obtain this result, instantiate the **AgnosticQuery** class, passing as an argument the SPARQL query string (QUERY_STRING), the time of interest (TIME) and the configuration file's path (CONFIG_PATH). For more information about the configuration file, see [Configuration file](#configuration-file). The specified time can be any time, not necessarily the exact time of a snapshot. In addition, it can be specified in any existing standard. **Please note**: only SELECT queries are allowed. Finally, execute the **run_agnostic_query** method.

``` python
  agnostic_query = AgnosticQuery(query=QUERY_STRING, on_time=TIME, config_path=CONFIG_PATH)
  agnostic_query.run_agnostic_query()
```

The output is a dictionary in which the key is the requested snapshot. The value corresponds to a set of tuples containing the query results at that snapshot. The positional value of the elements in the tuples is equivalent to the variables indicated in the query.

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

To obtain this result, instantiate the **AgnosticQuery** class, passing as an argument the SPARQL query string (QUERY_STRING) and the configuration file's path (CONFIG_PATH). For more information about the configuration file, see [Configuration file](#configuration-file). **Please note**: only SELECT queries are allowed. Finally, execute **the run_agnostic_query** method.

``` python
  agnostic_query = AgnosticQuery(query=QUERY_STRING, config_path=CONFIG_PATH)
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

<h3 id="configuration-file">Configuration file</h3>

The configuration file is mainly used to indicate to the library where to search for data and provenance. In addition, some optional values can be set to make executions faster and more efficient.

- **dataset** (required)
  - **triplestore_urls**: Specify a list of triplestore URLs containing data.  
  - **file_paths**: Specify a list of paths of files containing data.   
- **provenance** (required)
  - **triplestore_urls**: Specify a list of triplestore URLs containing provenance metadata.    
  - **file_paths** Specify a list of paths of files containing provenance metadata.      
- **blazegraph_full_text_search** (optional): Specify an affirmative Boolean value if Blazegraph was used as a triplestore, and a textual index was built to speed up queries. For more information, see [https://github.com/blazegraph/database/wiki/Rebuild_Text_Index_Procedure](https://github.com/blazegraph/database/wiki/Rebuild_Text_Index_Procedure). The allowed values are "true", "1", 1, "t", "y", "yes", "ok", or "false", "0", 0, "n", "f", "no".
- **cache_triplestore_url** (optional): Specifies the triplestore URL to use as a cache to make queries faster.

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
      "blazegraph_full_text_search": "yes",
      "cache_triplestore_url": "TRIPLESTORE_URL"
  }
```

## Developer's guide

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


