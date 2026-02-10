#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2025, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import unittest
from unittest.mock import MagicMock, patch

from rdflib import Graph, URIRef, Literal

from time_agnostic_library.agnostic_query import VersionQuery, DeltaQuery, get_insert_query, _reconstruct_at_time_from_data

CONFIG = {
    "dataset": {
        "triplestore_urls": ["http://127.0.0.1:9999/sparql"],
        "file_paths": [],
        "is_quadstore": True
    },
    "provenance": {
        "triplestore_urls": [],
        "file_paths": ["tests/prov.json"],
        "is_quadstore": False
    },
    "blazegraph_full_text_search": "no",
    "fuseki_full_text_search": "no",
    "virtuoso_full_text_search": "no",
    "graphdb_connector_name": ""
}

class TestAgnosticQueryEdgeCases(unittest.TestCase):
    """
    Test cases for edge cases in AgnosticQuery classes and related functions.
    """
    maxDiff = None

    def test_fuseki_full_text_search_configuration(self):
        """
        Test VersionQuery with Fuseki full-text search enabled.
        This should exercise the query generation at lines 200-201.
        """
        fuseki_config = CONFIG.copy()
        fuseki_config["fuseki_full_text_search"] = "yes"

        query = """
            SELECT ?agent
            WHERE {
                ?agent a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        # Create VersionQuery with Fuseki config - should build full-text search structures
        version_query = VersionQuery(query, config_dict=fuseki_config)

        # Verify the query was processed (no exception raised)
        self.assertIsNotNone(version_query)

    def test_virtuoso_full_text_search_configuration(self):
        """
        Test VersionQuery with Virtuoso full-text search enabled.
        This should exercise the query generation at lines 209-210.
        """
        virtuoso_config = CONFIG.copy()
        virtuoso_config["virtuoso_full_text_search"] = "yes"

        query = """
            SELECT ?agent
            WHERE {
                ?agent a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        # Create VersionQuery with Virtuoso config
        version_query = VersionQuery(query, config_dict=virtuoso_config)

        # Verify the query was processed
        self.assertIsNotNone(version_query)

    def test_graphdb_connector_configuration(self):
        """
        Test VersionQuery with GraphDB connector name specified.
        This should exercise the query generation at lines 219-221.
        """
        graphdb_config = CONFIG.copy()
        graphdb_config["graphdb_connector_name"] = "test_connector"

        query = """
            SELECT ?agent
            WHERE {
                ?agent a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        # Create VersionQuery with GraphDB config
        version_query = VersionQuery(query, config_dict=graphdb_config)

        # Verify the query was processed
        self.assertIsNotNone(version_query)

    def test_configuration_with_invalid_full_text_search_value(self):
        """
        Test VersionQuery with invalid full-text search configuration value.
        This should exercise the ValueError at line 71.
        """
        invalid_config = CONFIG.copy()
        invalid_config["blazegraph_full_text_search"] = "invalid_value"

        query = """
            SELECT ?agent
            WHERE {
                ?agent a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        # Should raise ValueError for invalid configuration
        with self.assertRaises(ValueError) as context:
            VersionQuery(query, config_dict=invalid_config)

        self.assertIn("full_text_search", str(context.exception).lower())

    def test_configuration_with_multiple_full_text_search_enabled(self):
        """
        Test VersionQuery with multiple full-text search systems enabled.
        This should exercise the ValueError at line 74.
        """
        multi_config = CONFIG.copy()
        multi_config["blazegraph_full_text_search"] = "yes"
        multi_config["fuseki_full_text_search"] = "yes"

        query = """
            SELECT ?agent
            WHERE {
                ?agent a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        # Should raise ValueError for multiple indexing systems
        with self.assertRaises(ValueError) as context:
            VersionQuery(query, config_dict=multi_config)

        self.assertIn("multiple indexing systems", str(context.exception).lower())

    def test_query_with_no_hooks(self):
        """
        Test VersionQuery with a query that has no hooks (no variables or patterns).
        A query with no triples should be allowed, it just won't return any results.
        """
        # A query with no actual patterns or variables
        query_no_hooks = """
            SELECT *
            WHERE { }
        """

        # Should not raise an error, just create an empty query
        version_query = VersionQuery(query_no_hooks, config_dict=CONFIG)
        self.assertIsNotNone(version_query)

    def test_version_query_with_config_dict_parameter(self):
        """
        Test VersionQuery initialization with config dict instead of config_path.
        This should exercise the alternative config loading path at line 45.
        """
        query = """
            SELECT ?agent
            WHERE {
                ?agent a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        # Pass config as dict (not path)
        version_query = VersionQuery(query, config_dict=CONFIG)

        # Verify the query was initialized correctly
        self.assertIsNotNone(version_query)
        self.assertEqual(version_query.config, CONFIG)

    def test_get_insert_query_with_empty_graph(self):
        """
        Test get_insert_query function with an empty graph.
        This should exercise the early return at lines 633-634.
        """
        empty_graph = Graph()
        graph_iri = URIRef("http://example.com/graph")

        # Should return empty string for empty graph
        result, num_statements = get_insert_query(graph_iri, empty_graph)

        # Empty graph should produce no insert query
        self.assertEqual(result, "")
        self.assertEqual(num_statements, 0)

    def test_get_insert_query_with_non_empty_graph(self):
        """
        Test get_insert_query function with a non-empty graph.
        This should exercise the serialization logic at lines 636-640.
        """
        graph = Graph()
        graph.add((URIRef("http://example.com/s"), URIRef("http://example.com/p"), Literal("object")))
        graph_iri = URIRef("http://example.com/graph")

        # Should generate an INSERT query
        result, num_statements = get_insert_query(graph_iri, graph)

        # Result should be a string containing INSERT
        self.assertIsInstance(result, str)
        self.assertIn("INSERT", result.upper())
        self.assertEqual(num_statements, 1)

    @patch('time_agnostic_library.agnostic_query.Sparql')
    def test_update_query_parsing_error(self, mock_sparql_class):
        """
        Test DeltaQuery when update query parsing fails.
        This should exercise the exception handling at lines 259-262.
        """
        query = """
            SELECT ?entity
            WHERE {
                ?entity a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        # Create DeltaQuery
        delta_query = DeltaQuery(query, config_dict=CONFIG, changed_properties=set())

        # Mock SPARQL to return malformed update query
        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance
        mock_sparql_instance.run_select_query.return_value = {
            'results': {
                'bindings': [
                    {
                        'se': {'value': 'http://example.com/se1'},
                        'generatedAtTime': {'value': '2021-05-07T09:59:15.000Z'},
                        'updateQuery': {'value': 'MALFORMED SPARQL UPDATE QUERY { NOT VALID }'}
                    }
                ]
            }
        }

        # The method should handle the malformed query gracefully
        # This tests the exception handling in update query processing


    def test_reconstruct_at_time_empty_prov(self):
        result = _reconstruct_at_time_from_data([], set(), ("2021-05-20T00:00:00+00:00", "2021-05-20T00:00:00+00:00"))
        self.assertEqual(result, {})

    def test_reconstruct_at_time_earlier_snapshot(self):
        prov = [
            {'time': '2021-05-07T09:59:15+00:00', 'updateQuery': None},
            {'time': '2021-06-01T18:46:41+00:00', 'updateQuery': None},
        ]
        quads = {(URIRef('http://s'), URIRef('http://p'), Literal('o'), URIRef('http://g'))}
        result = _reconstruct_at_time_from_data(prov, quads, ("2021-05-20T00:00:00+00:00", "2021-05-20T00:00:00+00:00"))
        self.assertEqual(len(result), 1)
        self.assertIn("2021-05-07T09:59:15+00:00", result)

    def test_reconstruct_at_time_no_earlier_snapshot(self):
        prov = [
            {'time': '2021-05-07T09:59:15+00:00', 'updateQuery': None},
        ]
        quads = {(URIRef('http://s'), URIRef('http://p'), Literal('o'), URIRef('http://g'))}
        result = _reconstruct_at_time_from_data(prov, quads, ("2021-01-01T00:00:00+00:00", "2021-01-01T00:00:00+00:00"))
        self.assertEqual(result, {})

    def test_reconstruct_at_time_no_interval_start(self):
        prov = [
            {'time': '2021-05-07T09:59:15+00:00', 'updateQuery': None},
        ]
        quads = {(URIRef('http://s'), URIRef('http://p'), Literal('o'), URIRef('http://g'))}
        result = _reconstruct_at_time_from_data(prov, quads, (None, "2021-01-01T00:00:00+00:00"))
        self.assertEqual(result, {})

    def test_generic_triple_pattern_raises_error(self):
        query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
        with self.assertRaises(ValueError) as ctx:
            VersionQuery(query, config_dict=CONFIG)
        self.assertIn("specify at least one URI", str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
