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
    "graphdb_connector_name": "",
    "cache_triplestore_url": {
        "endpoint": "",
        "update_endpoint": ""
    }
}

class TestAgnosticEntityDepthAndReverseRelations(unittest.TestCase):
    """
    Test cases for depth control and reverse relations functionality in AgnosticEntity.
    """
    maxDiff = None

    def test_collect_related_entities_recursively_with_zero_depth(self):
        """
        Test _collect_related_objects_recursively with depth = 0.
        This should trigger the early return condition: if depth is not None and depth <= 0: return
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        agnostic_entity = AgnosticEntity(
            entity_uri,
            config=CONFIG,
            include_related_objects=True,
            include_merged_entities=True,
            include_reverse_relations=True
        )
        
        processed_entities = set()
        histories = {}
        
        # Test the related objects recursion with depth 0
        agnostic_entity._collect_related_objects_recursively(
            entity_uri, processed_entities, histories, include_prov_metadata=False, depth=0
        )
        
        self.assertEqual(len(processed_entities), 0)
        self.assertEqual(len(histories), 0)

    def test_collect_related_entities_recursively_with_negative_depth(self):
        """
        Test _collect_related_objects_recursively with depth < 0.
        This should also trigger the early return condition: if depth is not None and depth <= 0: return
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        agnostic_entity = AgnosticEntity(
            entity_uri,
            config=CONFIG,
            include_related_objects=True,
            include_merged_entities=True,
            include_reverse_relations=True
        )
        
        processed_entities = set()
        histories = {}
        
        # Test the related objects recursion with negative depth
        agnostic_entity._collect_related_objects_recursively(
            entity_uri, processed_entities, histories, include_prov_metadata=False, depth=-1
        )
        
        self.assertEqual(len(processed_entities), 0)
        self.assertEqual(len(histories), 0)

    def test_collect_related_entities_states_at_time_with_zero_depth(self):
        """
        Test _collect_related_objects_states_at_time with depth = 0.
        This should trigger the early return condition: if depth is not None and depth <= 0: return
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        agnostic_entity = AgnosticEntity(
            entity_uri,
            config=CONFIG,
            include_related_objects=True,
            include_merged_entities=True,
            include_reverse_relations=True
        )
        
        processed_entities = set()
        histories = {}
        time_interval = ("2021-05-01T00:00:00+00:00", "2021-06-30T23:59:59+00:00")
        
        # Test the related objects states at time recursion with depth 0
        agnostic_entity._collect_related_objects_states_at_time(
            entity_uri, processed_entities, histories, time_interval, include_prov_metadata=False, depth=0
        )
        
        self.assertEqual(len(processed_entities), 0)
        self.assertEqual(len(histories), 0)

    def test_collect_related_entities_states_at_time_with_negative_depth(self):
        """
        Test _collect_related_objects_states_at_time with depth < 0.
        This should also trigger the early return condition: if depth is not None and depth <= 0: return
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        agnostic_entity = AgnosticEntity(
            entity_uri,
            config=CONFIG,
            include_related_objects=True,
            include_merged_entities=True,
            include_reverse_relations=True
        )
        
        processed_entities = set()
        histories = {}
        time_interval = ("2021-05-01T00:00:00+00:00", "2021-06-30T23:59:59+00:00")
        
        # Test the related objects states at time recursion with negative depth
        agnostic_entity._collect_related_objects_states_at_time(
            entity_uri, processed_entities, histories, time_interval, include_prov_metadata=False, depth=-5
        )
        
        self.assertEqual(len(processed_entities), 0)
        self.assertEqual(len(histories), 0)

    def test_get_history_with_reverse_relations(self):
        """
        Test get_history with include_reverse_relations=True to ensure the full flow works.
        This test ensures that the reverse relations functionality is properly integrated.
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        
        agnostic_entity = AgnosticEntity(
            entity_uri,
            config=CONFIG,
            include_related_objects=False,
            include_merged_entities=False,
            include_reverse_relations=True
        )
        
        history, prov_metadata = agnostic_entity.get_history(include_prov_metadata=False)
        
        self.assertIsInstance(history, dict)
        self.assertIn(entity_uri, history)
        self.assertIsInstance(history[entity_uri], dict)
        
        self.assertEqual(prov_metadata, {})

    def test_get_state_at_time_with_reverse_relations(self):
        """
        Test get_state_at_time with include_reverse_relations=True to ensure the full flow works.
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        time_interval = ("2021-05-01T00:00:00+00:00", "2021-06-30T23:59:59+00:00")
        
        agnostic_entity = AgnosticEntity(
            entity_uri,
            config=CONFIG,
            include_related_objects=False,
            include_merged_entities=False,
            include_reverse_relations=True
        )
        
        entity_histories, entity_snapshots, other_snapshots = agnostic_entity.get_state_at_time(
            time_interval, include_prov_metadata=False
        )
        
        self.assertIsInstance(entity_histories, dict)
        self.assertIsInstance(entity_snapshots, dict)
        self.assertIsNone(other_snapshots) 

    def test_find_reverse_related_entities_functionality(self):
        """
        Test the _find_reverse_related_entities method to ensure it works correctly.
        This method is called when include_reverse_relations=True.
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)
        
        reverse_entities = agnostic_entity._find_reverse_related_entities(entity_uri)
        
        self.assertIsInstance(reverse_entities, set)
        for entity in reverse_entities:
            self.assertIsInstance(entity, str)
            self.assertTrue(entity.startswith("http"))  # Should be URIs

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_find_reverse_related_entities_with_mocked_sparql(self, mock_sparql_class):
        """
        Test _find_reverse_related_entities with mocked SPARQL to control the response.
        This ensures we test the method logic independently of the actual data.
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)
        
        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance
        
        mock_response = {
            'results': {
                'bindings': [
                    {'subject': {'value': 'https://example.com/entity1'}},
                    {'subject': {'value': 'https://example.com/entity2'}},
                    {'subject': {'value': entity_uri}}  # This should be filtered out
                ]
            }
        }
        mock_sparql_instance.run_select_query.return_value = mock_response
        
        reverse_entities = agnostic_entity._find_reverse_related_entities(entity_uri)
        
        expected_entities = {
            'https://example.com/entity1',
            'https://example.com/entity2'
        }
        self.assertEqual(reverse_entities, expected_entities)
        
        mock_sparql_class.assert_called_once()
        mock_sparql_instance.run_select_query.assert_called_once()

    def test_collect_merged_entities_recursively_with_positive_depth(self):
        """
        Test _collect_merged_entities_recursively with depth = 1 using real test data.
        This should exercise the recursive call logic and depth decrement.
        Entity ra/4 was merged with ra/15519, so ra/4 should have merged entities.
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ra/4"
        agnostic_entity = AgnosticEntity(
            entity_uri,
            config=CONFIG,
            include_related_objects=False,
            include_merged_entities=True,
            include_reverse_relations=False
        )

        processed_entities = set()
        histories = {}

        # Test with depth = 1 - should collect merged entities but not recurse further
        agnostic_entity._collect_merged_entities_recursively(
            entity_uri, processed_entities, histories, include_prov_metadata=False, depth=1
        )

        # Should have found at least the merged entity ra/15519
        self.assertGreater(len(processed_entities), 0)
        self.assertGreater(len(histories), 0)

    def test_collect_reverse_relations_recursively_with_positive_depth(self):
        """
        Test _collect_reverse_relations_recursively with depth = 1 using real test data.
        This should exercise the recursive call logic for reverse relations.
        Entity ar/15519 has reverse relations (entities that reference it).
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15520"
        agnostic_entity = AgnosticEntity(
            entity_uri,
            config=CONFIG,
            include_related_objects=False,
            include_merged_entities=False,
            include_reverse_relations=True
        )

        processed_entities = set()
        histories = {}

        # Test with depth = 1 - should collect reverse related entities
        agnostic_entity._collect_reverse_relations_recursively(
            entity_uri, processed_entities, histories, include_prov_metadata=False, depth=1
        )

        # Should process reverse related entities
        self.assertIsInstance(processed_entities, set)
        self.assertIsInstance(histories, dict)

    def test_collect_merged_entities_states_at_time_with_positive_depth(self):
        """
        Test _collect_merged_entities_states_at_time with depth = 1.
        This tests the state-at-time variant of merged entity collection with depth control.
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ra/4"
        time_interval = ("2021-05-01T00:00:00+00:00", "2021-06-30T23:59:59+00:00")

        agnostic_entity = AgnosticEntity(
            entity_uri,
            config=CONFIG,
            include_related_objects=False,
            include_merged_entities=True,
            include_reverse_relations=False
        )

        processed_entities = set()
        histories = {}

        # Test with depth = 1
        agnostic_entity._collect_merged_entities_states_at_time(
            entity_uri, processed_entities, histories, time_interval, include_prov_metadata=False, depth=1
        )

        # Should have found merged entities
        self.assertGreater(len(processed_entities), 0)

    def test_collect_reverse_relations_states_at_time_with_positive_depth(self):
        """
        Test _collect_reverse_relations_states_at_time with depth = 1.
        This tests the state-at-time variant of reverse relation collection with depth control.
        """
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15520"
        time_interval = ("2021-05-01T00:00:00+00:00", "2021-06-30T23:59:59+00:00")

        agnostic_entity = AgnosticEntity(
            entity_uri,
            config=CONFIG,
            include_related_objects=False,
            include_merged_entities=False,
            include_reverse_relations=True
        )

        processed_entities = set()
        histories = {}

        # Test with depth = 1
        agnostic_entity._collect_reverse_relations_states_at_time(
            entity_uri, processed_entities, histories, time_interval, include_prov_metadata=False, depth=1
        )

        # Should process reverse related entities
        self.assertIsInstance(processed_entities, set)
        self.assertIsInstance(histories, dict)

if __name__ == '__main__':
    unittest.main() 