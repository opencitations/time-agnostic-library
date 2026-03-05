#!/usr/bin/env python
import os
import time
from collections import defaultdict
from pathlib import Path

from rdflib import Dataset
from sparqlite import SPARQLClient, SPARQLError

TRIPLESTORE = os.environ.get("TRIPLESTORE", "virtuoso")

_ENDPOINTS = {
    "virtuoso": "http://localhost:41720/sparql",
    "blazegraph": "http://localhost:41730/bigdata/namespace/tal/sparql",
    "fuseki": "http://localhost:41740/tal",
    "graphdb": "http://localhost:41750/repositories/tal",
}

ENDPOINT = _ENDPOINTS[TRIPLESTORE]

_UPDATE_ENDPOINTS = {
    "virtuoso": _ENDPOINTS["virtuoso"],
    "blazegraph": _ENDPOINTS["blazegraph"],
    "fuseki": _ENDPOINTS["fuseki"],
    "graphdb": "http://localhost:41750/repositories/tal/statements",
}

UPDATE_ENDPOINT = _UPDATE_ENDPOINTS[TRIPLESTORE]

_CHECK_QUERY = """
    ASK {
        GRAPH <https://github.com/arcangelo7/time_agnostic/br/> {
            <https://github.com/arcangelo7/time_agnostic/br/2>
            <http://purl.org/dc/terms/title>
            "Mapping the web relations of science centres and museums from Latin America"
        }
    }
"""

_VERIFY_QUERIES = [
    ('SELECT ?p ?o WHERE { GRAPH ?g { <https://github.com/arcangelo7/time_agnostic/br/31830> ?p ?o } }', 1),
    ('SELECT ?p ?o WHERE { GRAPH ?g { <https://github.com/arcangelo7/time_agnostic/id/27139> ?p ?o } }', 1),
    ('SELECT ?p ?o WHERE { GRAPH ?g { <https://github.com/arcangelo7/time_agnostic/ar/15519> ?p ?o } }', 1),
]


def wait_for_triplestore(endpoint=ENDPOINT, timeout=60):
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            raise TimeoutError(
                f"{TRIPLESTORE} did not become available within {timeout}s"
            )
        try:
            with SPARQLClient(endpoint) as client:
                client.ask("ASK { ?s ?p ?o }")
            return True
        except SPARQLError:
            time.sleep(1)


def check_data_exists(endpoint=ENDPOINT):
    try:
        with SPARQLClient(endpoint) as client:
            return client.ask(_CHECK_QUERY)
    except SPARQLError:
        return False


def _process_chunk(chunk, client, chunk_num, total):
    graphs = defaultdict(list)
    for s, p, o, c in chunk:
        graphs[c].append((s, p, o))

    insert_parts = []
    for graph, triples in graphs.items():
        triples_str = " . ".join(f"{s.n3()} {p.n3()} {o.n3()}" for s, p, o in triples)
        graph_uri = graph.n3().strip("[]")
        insert_parts.append(f"GRAPH {graph_uri} {{ {triples_str} . }}")

    try:
        client.update(f"INSERT DATA {{ {' '.join(insert_parts)} }}")
        print(f"Loaded chunk {chunk_num}/{total}")
        return True
    except SPARQLError as e:
        print(f"Error loading chunk: {e}")
        return False


def load_data(data_file, update_endpoint=UPDATE_ENDPOINT):
    print(f"Loading data from {data_file} into {TRIPLESTORE}...")

    g = Dataset(default_union=True)
    g.parse(data_file, format="nquads")

    chunk_size = 1000
    triples = list(g.quads())
    total_chunks = (len(triples) + chunk_size - 1) // chunk_size

    with SPARQLClient(update_endpoint) as client:
        for i in range(0, len(triples), chunk_size):
            _process_chunk(triples[i:i + chunk_size], client, (i // chunk_size) + 1, total_chunks)


def _verify_data_loaded(endpoint=ENDPOINT, timeout=60):
    start_time = time.time()
    with SPARQLClient(endpoint) as client:
        for query, min_results in _VERIFY_QUERIES:
            while True:
                if time.time() - start_time > timeout:
                    raise TimeoutError(f"Data verification timed out: {query}")
                result = client.query(query)
                if len(result["results"]["bindings"]) >= min_results:
                    break
                time.sleep(1)
    print("Data verification passed.")


def main():
    print(f"Setting up {TRIPLESTORE} at {ENDPOINT}...")

    print(f"Waiting for {TRIPLESTORE} to be ready...")
    wait_for_triplestore()

    print("Checking if test data is already loaded...")
    if check_data_exists():
        print("Test data already present in the database.")
        return

    print("Test data not found in the database. Loading...")
    data_file = Path(__file__).parent / "kb" / "data.nq"
    if not data_file.exists():
        print(f"Error: Test data file not found at {data_file}")
        return

    load_data(data_file)
    _verify_data_loaded()
    print("Data loading completed.")


if __name__ == "__main__":
    main()
