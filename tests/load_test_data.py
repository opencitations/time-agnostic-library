#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Test data loading script for the time-agnostic-library test suite."""

import time
from pathlib import Path
from collections import defaultdict
from rdflib import ConjunctiveGraph
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from SPARQLWrapper.SPARQLExceptions import (
    EndPointNotFound,
    QueryBadFormed,
    EndPointInternalError,
    SPARQLWrapperException
)


def wait_for_virtuoso(endpoint="http://localhost:9999/sparql", timeout=30):
    """Wait for Virtuoso to be ready."""
    start_time = time.time()
    while True:
        if time.time() - start_time > timeout:
            raise TimeoutError(
                "Virtuoso did not become available within the timeout period"
            )

        try:
            sparql = SPARQLWrapper(endpoint)
            sparql.setQuery("ASK { ?s ?p ?o }")
            sparql.setReturnFormat(JSON)
            sparql.query()
            return True
        except (EndPointNotFound, SPARQLWrapperException):
            time.sleep(1)


def check_data_exists(endpoint="http://localhost:9999/sparql"):
    """Check if test data is already loaded."""
    try:
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(
            """
        ASK {
            GRAPH <https://github.com/arcangelo7/time_agnostic/br/> {
                <https://github.com/arcangelo7/time_agnostic/br/2>
                <http://purl.org/dc/terms/title>
                "Mapping the web relations of science centres and museums from Latin America"
            }
        }
        """
        )
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        return results.get("boolean", False)
    except (EndPointNotFound, QueryBadFormed, SPARQLWrapperException):
        return False


def _process_chunk(chunk, sparql, chunk_num, total):
    """Process a chunk of triples and insert them into the database."""
    # Group triples by graph
    graphs = defaultdict(list)
    for s, p, o, c in chunk:
        graphs[c].append((s, p, o))

    # Construct query with graph contexts
    insert_parts = []
    for graph, triples in graphs.items():
        # Create N3 representations and join them
        triples_str = " . ".join(f"{s.n3()} {p.n3()} {o.n3()}" for s, p, o in triples)
        graph_uri = graph.n3().strip("[]")
        insert_parts.append(f"GRAPH {graph_uri} {{ {triples_str} . }}")

    # Execute the query
    sparql.setQuery(f"INSERT DATA {{ {' '.join(insert_parts)} }}")

    try:
        sparql.query()
        print(f"Loaded chunk {chunk_num}/{total}")
        return True
    except (EndPointNotFound, QueryBadFormed, EndPointInternalError) as e:
        print(f"Error loading chunk: {e}")
        return False


def load_data(data_file, endpoint="http://localhost:9999/sparql"):
    """Load data from .nq file into Virtuoso."""
    print(f"Loading data from {data_file}...")

    # Parse file and prepare data
    g = ConjunctiveGraph()
    g.parse(data_file, format="nquads")

    # Setup SPARQL connection
    sparql = SPARQLWrapper(endpoint)
    sparql.setMethod(POST)
    sparql.setReturnFormat(JSON)

    # Split and process in chunks
    chunk_size = 1000
    triples = list(g.quads())
    total_chunks = (len(triples) + chunk_size - 1) // chunk_size

    for i in range(0, len(triples), chunk_size):
        _process_chunk(triples[i:i + chunk_size], sparql, (i // chunk_size) + 1, total_chunks)


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

    # Load the data
    data_file = Path(__file__).parent / "kb" / "data.nq"
    if not data_file.exists():
        print(f"Error: Test data file not found at {data_file}")
        return

    load_data(data_file)
    print("Data loading completed.")


if __name__ == "__main__":
    main()
