#!/usr/bin/python
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

from unittest.mock import MagicMock, patch

from triplestore_config import CONFIG

from time_agnostic_library.agnostic_entity import AgnosticEntity


class TestAgnosticEntityDepthAndReverseRelations:

    def test_collect_related_entities_recursively_with_zero_depth(self):
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

        agnostic_entity._collect_related_objects_recursively(
            entity_uri, processed_entities, histories, include_prov_metadata=False, depth=0
        )

        assert len(processed_entities) == 0
        assert len(histories) == 0

    def test_collect_related_entities_recursively_with_negative_depth(self):
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

        agnostic_entity._collect_related_objects_recursively(
            entity_uri, processed_entities, histories, include_prov_metadata=False, depth=-1
        )

        assert len(processed_entities) == 0
        assert len(histories) == 0

    def test_collect_related_entities_states_at_time_with_zero_depth(self):
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

        agnostic_entity._collect_related_objects_states_at_time(
            entity_uri, processed_entities, histories, time_interval, include_prov_metadata=False, depth=0
        )

        assert len(processed_entities) == 0
        assert len(histories) == 0

    def test_collect_related_entities_states_at_time_with_negative_depth(self):
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

        agnostic_entity._collect_related_objects_states_at_time(
            entity_uri, processed_entities, histories, time_interval, include_prov_metadata=False, depth=-5
        )

        assert len(processed_entities) == 0
        assert len(histories) == 0

    def test_get_history_with_reverse_relations(self):
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"

        agnostic_entity = AgnosticEntity(
            entity_uri,
            config=CONFIG,
            include_related_objects=False,
            include_merged_entities=False,
            include_reverse_relations=True
        )

        history, prov_metadata = agnostic_entity.get_history(include_prov_metadata=False)

        assert isinstance(history, dict)
        assert entity_uri in history
        assert isinstance(history[entity_uri], dict)

        assert prov_metadata == {}

    def test_get_state_at_time_with_reverse_relations(self):
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

        assert isinstance(entity_histories, dict)
        assert isinstance(entity_snapshots, dict)
        assert other_snapshots is None

    def test_find_reverse_related_entities_functionality(self):
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

        reverse_entities = agnostic_entity._find_reverse_related_entities(entity_uri)

        assert isinstance(reverse_entities, set)
        for entity in reverse_entities:
            assert isinstance(entity, str)
            assert entity.startswith("http")

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_find_reverse_related_entities_with_mocked_sparql(self, mock_sparql_class):
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance

        mock_response = {
            'results': {
                'bindings': [
                    {'subject': {'value': 'https://example.com/entity1'}},
                    {'subject': {'value': 'https://example.com/entity2'}},
                    {'subject': {'value': entity_uri}}
                ]
            }
        }
        mock_sparql_instance.run_select_query.return_value = mock_response

        reverse_entities = agnostic_entity._find_reverse_related_entities(entity_uri)

        expected_entities = {
            'https://example.com/entity1',
            'https://example.com/entity2'
        }
        assert reverse_entities == expected_entities

        mock_sparql_class.assert_called_once()
        mock_sparql_instance.run_select_query.assert_called_once()

    def test_collect_merged_entities_recursively_with_positive_depth(self):
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

        agnostic_entity._collect_merged_entities_recursively(
            entity_uri, processed_entities, histories, include_prov_metadata=False, depth=1
        )

        assert len(processed_entities) > 0
        assert len(histories) > 0

    def test_collect_reverse_relations_recursively_with_positive_depth(self):
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

        agnostic_entity._collect_reverse_relations_recursively(
            entity_uri, processed_entities, histories, include_prov_metadata=False, depth=1
        )

        assert isinstance(processed_entities, set)
        assert isinstance(histories, dict)

    def test_collect_merged_entities_states_at_time_with_positive_depth(self):
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

        agnostic_entity._collect_merged_entities_states_at_time(
            entity_uri, processed_entities, histories, time_interval, include_prov_metadata=False, depth=1
        )

        assert len(processed_entities) > 0

    def test_collect_reverse_relations_states_at_time_with_positive_depth(self):
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

        agnostic_entity._collect_reverse_relations_states_at_time(
            entity_uri, processed_entities, histories, time_interval, include_prov_metadata=False, depth=1
        )

        assert isinstance(processed_entities, set)
        assert isinstance(histories, dict)
