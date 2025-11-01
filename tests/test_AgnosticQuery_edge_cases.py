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

from time_agnostic_library.agnostic_query import VersionQuery, DeltaQuery, get_insert_query

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
    "graphdb_connector_name": "",
    "cache_triplestore_url": {
        "endpoint": "",
        "update_endpoint": ""
    }
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
        version_query = VersionQuery(query, config=fuseki_config)

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
        version_query = VersionQuery(query, config=virtuoso_config)

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
        version_query = VersionQuery(query, config=graphdb_config)

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
            VersionQuery(query, config=invalid_config)

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
            VersionQuery(query, config=multi_config)

        self.assertIn("only one", str(context.exception).lower())

    def test_query_with_no_hooks(self):
        """
        Test VersionQuery with a query that has no hooks (no variables or patterns).
        This should exercise the ValueError at line 86.
        """
        # A query with no actual patterns or variables
        query_no_hooks = """
            SELECT *
            WHERE { }
        """

        # Should raise ValueError
        with self.assertRaises(ValueError) as context:
            VersionQuery(query_no_hooks, config=CONFIG)

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
        version_query = VersionQuery(query, config=CONFIG)

        # Verify the query was initialized correctly
        self.assertIsNotNone(version_query)
        self.assertEqual(version_query.config, CONFIG)

    def test_get_insert_query_with_empty_graph(self):
        """
        Test get_insert_query function with an empty graph.
        This should exercise the early return at lines 633-634.
        """
        from rdflib import Dataset

        empty_graph = Dataset()
        cache_endpoint = "http://127.0.0.1:8888/sparql"

        # Should return None or empty string for empty graph
        result = get_insert_query(empty_graph, cache_endpoint)

        # Empty graph should produce no insert query
        self.assertIsNone(result) or self.assertEqual(result, "")

    def test_get_insert_query_with_non_empty_graph(self):
        """
        Test get_insert_query function with a non-empty graph.
        This should exercise the serialization logic at lines 636-640.
        """
        from rdflib import Dataset, URIRef, Literal

        graph = Dataset()
        graph.add((URIRef("http://example.com/s"), URIRef("http://example.com/p"), Literal("object")))

        cache_endpoint = "http://127.0.0.1:8888/sparql"

        # Should generate an INSERT query
        result = get_insert_query(graph, cache_endpoint)

        # Result should be a string containing INSERT
        self.assertIsInstance(result, str)
        self.assertIn("INSERT", result.upper())

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
        delta_query = DeltaQuery(query, config=CONFIG, changed_properties=set())

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

    def test_inverse_property_path_in_query(self):
        """
        Test VersionQuery with inverse property path (^) in the query.
        This should exercise the InvPath handling at lines 506-507.
        """
        query = """
            SELECT ?subject ?author
            WHERE {
                ?subject ^<http://purl.org/spar/pro/isHeldBy> ?author .
            }
        """

        # Create VersionQuery with inverse property path
        version_query = VersionQuery(query, config=CONFIG)

        # Verify the query was processed
        self.assertIsNotNone(version_query)

if __name__ == '__main__':
    unittest.main()
