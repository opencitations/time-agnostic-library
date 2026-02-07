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

from time_agnostic_library.agnostic_entity import AgnosticEntity

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

class TestAgnosticEntityEdgeCases(unittest.TestCase):
    """
    Test cases for edge cases in AgnosticEntity, including empty results and error handling.
    """
    maxDiff = None

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_get_state_at_time_with_no_snapshots(self, mock_sparql_class):
        """
        Test _get_entity_state_at_time when entity has no snapshots.
        This should trigger the early return at line 560: if not bindings: return {}, {}, other_snapshots_metadata
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/nonexistent/entity"
        time_interval = ("2021-05-01T00:00:00+00:00", "2021-06-30T23:59:59+00:00")

        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

        # Mock SPARQL to return empty results
        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance
        mock_sparql_instance.run_select_query.return_value = {
            'results': {'bindings': []}
        }

        entity_graphs, entity_snapshots, other_snapshots = agnostic_entity._get_entity_state_at_time(
            time_interval, include_prov_metadata=False
        )

        self.assertEqual(entity_graphs, {})
        self.assertEqual(entity_snapshots, {})
        self.assertEqual(other_snapshots, {})

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_get_state_at_time_with_no_relevant_results_before_time(self, mock_sparql_class):
        """
        Test _get_entity_state_at_time when no snapshots exist before the requested time.
        This should trigger the return at line 584: return {}, {}, other_snapshots_metadata
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/test/entity"
        # Request time before any snapshots exist
        time_interval = ("2020-01-01T00:00:00+00:00", "2020-01-31T23:59:59+00:00")

        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

        # Mock SPARQL to return snapshots after the requested time
        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance
        mock_sparql_instance.run_select_query.return_value = {
            'results': {
                'bindings': [
                    {
                        'snapshot': {'value': 'https://example.com/snapshot1'},
                        'time': {'value': '2021-05-07T09:59:15.000Z'},
                        'responsibleAgent': {'value': 'https://orcid.org/0000-0002-8420-0696'}
                    }
                ]
            }
        }

        entity_graphs, entity_snapshots, other_snapshots = agnostic_entity._get_entity_state_at_time(
            time_interval, include_prov_metadata=False
        )

        self.assertEqual(entity_graphs, {})
        self.assertEqual(entity_snapshots, {})

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_get_state_at_time_with_no_relevant_results_and_no_start_time(self, mock_sparql_class):
        """
        Test _get_entity_state_at_time when no relevant results and interval start is None.
        This should trigger the return at line 586: return {}, {}, other_snapshots_metadata
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/test/entity"
        # Time interval with None start
        time_interval = (None, "2020-01-31T23:59:59+00:00")

        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

        # Mock SPARQL to return snapshots after the requested time
        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance
        mock_sparql_instance.run_select_query.return_value = {
            'results': {
                'bindings': [
                    {
                        'snapshot': {'value': 'https://example.com/snapshot1'},
                        'time': {'value': '2021-05-07T09:59:15.000Z'},
                        'responsibleAgent': {'value': 'https://orcid.org/0000-0002-8420-0696'}
                    }
                ]
            }
        }

        entity_graphs, entity_snapshots, other_snapshots = agnostic_entity._get_entity_state_at_time(
            time_interval, include_prov_metadata=False
        )

        self.assertEqual(entity_graphs, {})
        self.assertEqual(entity_snapshots, {})


    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_find_merged_entities_with_sparql_error(self, mock_sparql_class):
        """
        Test _find_merged_entities when SPARQL execution raises an exception.
        This should exercise the exception handling at lines 932-933.
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/test/entity"
        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

        # Mock SPARQL to raise an exception
        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance
        mock_sparql_instance.run_select_query.side_effect = Exception("SPARQL execution error")

        # Should return empty set on error
        merged_entities = agnostic_entity._find_merged_entities(entity_uri)

        self.assertEqual(merged_entities, set())

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_find_reverse_related_entities_with_sparql_error(self, mock_sparql_class):
        """
        Test _find_reverse_related_entities when SPARQL execution raises an exception.
        This should exercise the exception handling at lines 973-974.
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/test/entity"
        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

        # Mock SPARQL to raise an exception
        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance
        mock_sparql_instance.run_select_query.side_effect = Exception("SPARQL execution error")

        # Should return empty set on error
        reverse_entities = agnostic_entity._find_reverse_related_entities(entity_uri)

        self.assertEqual(reverse_entities, set())

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_query_dataset_with_non_quadstore(self, mock_sparql_class):
        """
        Test _query_dataset with is_quadstore=False configuration.
        This should exercise the query construction at line 859-865 (non-quadstore path).
        """
        non_quadstore_config = CONFIG.copy()
        non_quadstore_config["dataset"] = {
            "triplestore_urls": ["http://127.0.0.1:9999/sparql"],
            "file_paths": [],
            "is_quadstore": False  # Test non-quadstore configuration
        }

        entity_uri = "https://github.com/arcangelo7/time_agnostic/test/entity"
        agnostic_entity = AgnosticEntity(entity_uri, config=non_quadstore_config)

        # Mock SPARQL to return empty graph
        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance

        from rdflib import Dataset
        mock_sparql_instance.run_construct_query.return_value = Dataset()

        # Call the method - should use non-quadstore query
        result = agnostic_entity._query_dataset(entity_uri)

        # Verify SPARQL was called
        mock_sparql_class.assert_called_once()
        call_args = mock_sparql_class.call_args
        query = call_args[0][0]

        # Verify the query doesn't contain GRAPH clause (non-quadstore)
        self.assertNotIn("GRAPH ?g", query)
        self.assertIn("SELECT DISTINCT ?s ?p ?o", query)
        self.assertNotIn("?g", query)  # No graph variable in non-quadstore query

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_find_reverse_related_entities_with_non_quadstore(self, mock_sparql_class):
        """
        Test _find_reverse_related_entities with is_quadstore=False configuration.
        This should exercise the query construction at line 957-963 (non-quadstore path).
        """
        non_quadstore_config = CONFIG.copy()
        non_quadstore_config["dataset"] = {
            "triplestore_urls": ["http://127.0.0.1:9999/sparql"],
            "file_paths": [],
            "is_quadstore": False  # Test non-quadstore configuration
        }

        entity_uri = "https://github.com/arcangelo7/time_agnostic/test/entity"
        agnostic_entity = AgnosticEntity(entity_uri, config=non_quadstore_config)

        # Mock SPARQL to return some results
        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance
        mock_sparql_instance.run_select_query.return_value = {
            'results': {
                'bindings': [
                    {'subject': {'value': 'https://example.com/reverse1'}},
                    {'subject': {'value': 'https://example.com/reverse2'}},
                ]
            }
        }

        # Call the method - should use non-quadstore query
        result = agnostic_entity._find_reverse_related_entities(entity_uri)

        # Verify results
        self.assertEqual(result, {'https://example.com/reverse1', 'https://example.com/reverse2'})

        # Verify SPARQL was called with non-quadstore query
        mock_sparql_class.assert_called_once()
        call_args = mock_sparql_class.call_args
        query = call_args[0][0]

        # Verify the query doesn't contain GRAPH clause (non-quadstore)
        self.assertNotIn("GRAPH", query)
        self.assertIn("SELECT DISTINCT ?subject", query)

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_filter_timestamps_with_missing_time_value(self, mock_sparql_class):
        """
        Test _filter_timestamps_by_interval when time binding has no value.
        This should exercise the continue statement at lines 990-992.
        """
        from time_agnostic_library.agnostic_entity import _filter_timestamps_by_interval

        # Create test data with one entry missing 'value' in time binding
        iterator = [
            {'time': {'value': '2021-05-07T09:59:15.000Z'}},
            {'time': {}},  # Missing 'value' key - should be skipped at line 990
            {'time': {'value': '2021-06-01T18:46:41.000Z'}},
        ]

        interval = ("2021-05-01T00:00:00+00:00", "2021-06-30T23:59:59+00:00")

        result = _filter_timestamps_by_interval(interval, iterator, time_index='time')

        # Should only return entries with valid time values
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['time']['value'], '2021-05-07T09:59:15.000Z')
        self.assertEqual(result[1]['time']['value'], '2021-06-01T18:46:41.000Z')

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_filter_timestamps_with_missing_time_index(self, mock_sparql_class):
        """
        Test _filter_timestamps_by_interval when time_index is not in timestamp.
        This should exercise the continue statement at lines 991-992.
        """
        from time_agnostic_library.agnostic_entity import _filter_timestamps_by_interval

        # Create test data where time_index 'timestamp' doesn't exist
        iterator = [
            {'time': {'value': '2021-05-07T09:59:15.000Z'}},  # Wrong key
            {'other_key': {'value': '2021-06-01T18:46:41.000Z'}},  # Wrong key
        ]

        interval = ("2021-05-01T00:00:00+00:00", "2021-06-30T23:59:59+00:00")

        # Using time_index='timestamp' which doesn't exist in the data
        result = _filter_timestamps_by_interval(interval, iterator, time_index='timestamp')

        # Should return empty list as no entries have the correct time_index
        self.assertEqual(len(result), 0)

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_filter_timestamps_with_only_after_time(self, mock_sparql_class):
        """
        Test _filter_timestamps_by_interval with only after_time (no before_time).
        This should exercise the conditional at lines 996-998.
        """
        from time_agnostic_library.agnostic_entity import _filter_timestamps_by_interval

        iterator = [
            {'time': {'value': '2021-05-07T09:59:15.000Z'}},
            {'time': {'value': '2021-06-01T18:46:41.000Z'}},
            {'time': {'value': '2021-07-15T12:00:00.000Z'}},
        ]

        # Only after_time, no before_time
        interval = ("2021-06-01T00:00:00+00:00", None)

        result = _filter_timestamps_by_interval(interval, iterator, time_index='time')

        # Should return only entries after June 1st
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['time']['value'], '2021-06-01T18:46:41.000Z')
        self.assertEqual(result[1]['time']['value'], '2021-07-15T12:00:00.000Z')


    def test_manage_update_queries_with_malformed_query(self):
        """
        Test _manage_update_queries with a malformed SPARQL update query.
        This should exercise the exception handling at lines 826-828.
        """
        from rdflib import Dataset

        graph = Dataset()

        # Malformed SPARQL query
        malformed_query = "THIS IS NOT VALID SPARQL { DELETE SOMETHING }"

        # Should handle the exception gracefully and print error
        # The method doesn't raise, it just prints
        AgnosticEntity._manage_update_queries(graph, malformed_query)

        # Graph should remain unchanged
        self.assertEqual(len(graph), 0)

    def test_include_prov_metadata_with_was_derived_from(self):
        """
        Test _include_prov_metadata when processing wasDerivedFrom properties.
        This should exercise the list initialization at line 653 and list handling.
        """
        from rdflib import Dataset, URIRef, Literal
        from rdflib.namespace import XSD
        from time_agnostic_library.prov_entity import ProvEntity

        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

        # Create current state graph with provenance data including wasDerivedFrom
        current_state = Dataset()
        snapshot1 = URIRef("https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1")
        snapshot2 = URIRef("https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2")
        snapshot3 = URIRef("https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3")
        time1 = Literal("2021-05-07T09:59:15.000Z", datatype=XSD.dateTime)

        # Add triples for snapshot with multiple wasDerivedFrom
        current_state.add((snapshot1, ProvEntity.iri_generated_at_time, time1))
        current_state.add((snapshot2, ProvEntity.iri_was_derived_from, snapshot1))
        current_state.add((snapshot3, ProvEntity.iri_was_derived_from, snapshot2))

        triples_generated_at_time = [(snapshot1, ProvEntity.iri_generated_at_time, time1)]

        # Call the method (only takes 2 parameters: triples_generated_at_time and current_state)
        # The method creates its own prov_metadata structure
        result = agnostic_entity._include_prov_metadata(triples_generated_at_time, current_state)

        # Verify result is a dictionary
        self.assertIsInstance(result, dict)

if __name__ == '__main__':
    unittest.main()
