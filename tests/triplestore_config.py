import os

TRIPLESTORE = os.environ.get("TRIPLESTORE", "virtuoso")

_ENDPOINTS = {
    "virtuoso": "http://127.0.0.1:41720/sparql",
    "blazegraph": "http://127.0.0.1:41730/bigdata/namespace/tal/sparql",
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
}

CONFIG_FTS = {**CONFIG, _FTS_KEY[TRIPLESTORE]: "yes"}
