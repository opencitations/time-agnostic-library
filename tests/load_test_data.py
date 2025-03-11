#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from pathlib import Path
from collections import defaultdict
from rdflib import ConjunctiveGraph
from SPARQLWrapper import SPARQLWrapper, JSON, POST


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
        except Exception:
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
    except Exception:
        return False


def load_data(data_file, endpoint="http://localhost:9999/sparql"):
    """Load data from .nq file into Virtuoso."""
    print(f"Loading data from {data_file}...")

    # Read the .nq file into an RDFLib ConjunctiveGraph
    g = ConjunctiveGraph()
    g.parse(data_file, format="nquads")

    # Split into smaller chunks to avoid timeout
    chunk_size = 1000
    triples = list(g.quads())
    total_chunks = (len(triples) + chunk_size - 1) // chunk_size

    sparql = SPARQLWrapper(endpoint)
    sparql.setMethod(POST)
    sparql.setReturnFormat(JSON)

    for i in range(0, len(triples), chunk_size):
        chunk = triples[i : i + chunk_size]

        # Group triples by graph
        graphs = defaultdict(list)
        for s, p, o, c in chunk:
            graphs[c].append((s, p, o))  # Keep c as RDFLib Node

        # Construct query with proper graph contexts
        insert_parts = []
        for graph, triples in graphs.items():
            graph_triples = []
            for s, p, o in triples:
                triple = f"{s.n3()} {p.n3()} {o.n3()}"
                graph_triples.append(triple)

            # Remove any square brackets around the graph URI
            graph_uri = graph.n3().strip("[]")
            graph_insert = f"GRAPH {graph_uri} {{ {' . '.join(graph_triples)} . }}"
            insert_parts.append(graph_insert)

        insert_query = f"""
        INSERT DATA {{
            {' '.join(insert_parts)}
        }}
        """

        sparql.setQuery(insert_query)
        try:
            sparql.query()
            print(f"Loaded chunk {(i//chunk_size)+1}/{total_chunks}")
        except Exception as e:
            print(f"Error loading chunk: {e}")
            continue


def main():
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
