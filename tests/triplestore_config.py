import os

TRIPLESTORE = os.environ.get("TRIPLESTORE", "virtuoso")

_ENDPOINTS = {
    "virtuoso": "http://127.0.0.1:41720/sparql",
    "blazegraph": "http://127.0.0.1:41730/bigdata/namespace/tal/sparql",
    "fuseki": "http://127.0.0.1:41740/tal",
    "graphdb": "http://127.0.0.1:41750/repositories/tal",
    "qlever": "http://127.0.0.1:41760",
}

ENDPOINT = _ENDPOINTS[TRIPLESTORE]

CONFIG = {
    "dataset": {
        "triplestore_urls": [ENDPOINT],
        "file_paths": [],
        "is_quadstore": True,
    },
    "provenance": {
        "triplestore_urls": [],
        "file_paths": ["tests/prov.json"],
        "is_quadstore": False,
    },
    "blazegraph_full_text_search": "no",
    "fuseki_full_text_search": "no",
    "virtuoso_full_text_search": "no",
    "graphdb_connector_name": "",
}

CONFIG_PROV_IN_TRIPLESTORE = {
    "dataset": {
        "triplestore_urls": [ENDPOINT],
        "file_paths": [],
        "is_quadstore": True,
    },
    "provenance": {
        "triplestore_urls": [ENDPOINT],
        "file_paths": [],
        "is_quadstore": True,
    },
    "blazegraph_full_text_search": "no",
    "fuseki_full_text_search": "no",
    "virtuoso_full_text_search": "no",
    "graphdb_connector_name": "",
}

_FTS_KEY = {
    "virtuoso": "virtuoso_full_text_search",
    "blazegraph": "blazegraph_full_text_search",
    "fuseki": "fuseki_full_text_search",
    "graphdb": "graphdb_connector_name",
}

_FTS_VALUE = {
    "virtuoso": "yes",
    "blazegraph": "yes",
    "fuseki": "yes",
    "graphdb": "tal_fts",
}

CONFIG_FTS = {**CONFIG, _FTS_KEY[TRIPLESTORE]: _FTS_VALUE[TRIPLESTORE]} if TRIPLESTORE in _FTS_KEY else CONFIG.copy()
