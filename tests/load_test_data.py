#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Test data loading script for the time-agnostic-library test suite."""

import time
from collections import defaultdict
from pathlib import Path

from rdflib import Dataset
from sparqlite import SPARQLClient, SPARQLError


def wait_for_virtuoso(endpoint="http://localhost:9999/sparql", timeout=30):
    """Wait for Virtuoso to be ready."""
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            raise TimeoutError(
                "Virtuoso did not become available within the timeout period"
            )

        try:
            with SPARQLClient(endpoint) as client:
                client.ask("ASK { ?s ?p ?o }")
            return True
        except SPARQLError:
            time.sleep(1)


def check_data_exists(endpoint="http://localhost:9999/sparql"):
    """Check if test data is already loaded."""
    try:
        with SPARQLClient(endpoint) as client:
            return client.ask("""
                ASK {
                    GRAPH <https://github.com/arcangelo7/time_agnostic/br/> {
                        <https://github.com/arcangelo7/time_agnostic/br/2>
                        <http://purl.org/dc/terms/title>
                        "Mapping the web relations of science centres and museums from Latin America"
                    }
                }
            """)
    except SPARQLError:
        return False


def _process_chunk(chunk, client, chunk_num, total):
    """Process a chunk of triples and insert them into the database."""
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


def load_data(data_file, endpoint="http://localhost:9999/sparql"):
    """Load data from .nq file into Virtuoso."""
    print(f"Loading data from {data_file}...")

    g = Dataset(default_union=True)
    g.parse(data_file, format="nquads")

    chunk_size = 1000
    triples = list(g.quads())
    total_chunks = (len(triples) + chunk_size - 1) // chunk_size

    with SPARQLClient(endpoint) as client:
        for i in range(0, len(triples), chunk_size):
            _process_chunk(triples[i:i + chunk_size], client, (i // chunk_size) + 1, total_chunks)


def main():
    """Main entry point to load test data into the Virtuoso database."""
    # Wait for Virtuoso to be ready
    print("Waiting for Virtuoso to be ready...")
    wait_for_virtuoso()

    # Check if data already exists
    print("Checking if test data is already loaded...")
    if check_data_exists():
        print("Test data already present in the database.")
        return
    else:
        print("Test data not found in the database. Loading...")

    # Load the data
    data_file = Path(__file__).parent / "kb" / "data.nq"
    if not data_file.exists():
        print(f"Error: Test data file not found at {data_file}")
        return

    load_data(data_file)
    print("Data loading completed.")


if __name__ == "__main__":
    main()
