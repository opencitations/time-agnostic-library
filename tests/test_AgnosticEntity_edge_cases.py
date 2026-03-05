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

from time_agnostic_library.agnostic_entity import (
    AgnosticEntity,
    _compose_update_queries,
    _fast_parse_update,
    _filter_timestamps_by_interval,
    _find_matching_close_brace,
    _find_related_object_uris,
)
from time_agnostic_library.prov_entity import ProvEntity


class TestAgnosticEntityEdgeCases:

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_get_state_at_time_with_no_snapshots(self, mock_sparql_class):
        entity_uri = "https://github.com/arcangelo7/time_agnostic/nonexistent/entity"
        time_interval = ("2021-05-01T00:00:00+00:00", "2021-06-30T23:59:59+00:00")

        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance
        mock_sparql_instance.run_select_query.return_value = {
            'results': {'bindings': []}
        }

        entity_graphs, entity_snapshots, other_snapshots = agnostic_entity._get_entity_state_at_time(
            time_interval, include_prov_metadata=False
        )

        assert entity_graphs == {}
        assert entity_snapshots == {}
        assert other_snapshots == {}

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_get_state_at_time_with_no_relevant_results_before_time(self, mock_sparql_class):
        entity_uri = "https://github.com/arcangelo7/time_agnostic/test/entity"
        time_interval = ("2020-01-01T00:00:00+00:00", "2020-01-31T23:59:59+00:00")

        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

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

        entity_graphs, entity_snapshots, _other_snapshots = agnostic_entity._get_entity_state_at_time(
            time_interval, include_prov_metadata=False
        )

        assert entity_graphs == {}
        assert entity_snapshots == {}

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_get_state_at_time_with_no_relevant_results_and_no_start_time(self, mock_sparql_class):
        entity_uri = "https://github.com/arcangelo7/time_agnostic/test/entity"
        time_interval = (None, "2020-01-31T23:59:59+00:00")

        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

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

        entity_graphs, entity_snapshots, _other_snapshots = agnostic_entity._get_entity_state_at_time(
            time_interval, include_prov_metadata=False
        )

        assert entity_graphs == {}
        assert entity_snapshots == {}

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_find_merged_entities_with_sparql_error(self, mock_sparql_class):
        entity_uri = "https://github.com/arcangelo7/time_agnostic/test/entity"
        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance
        mock_sparql_instance.run_select_query.side_effect = Exception("SPARQL execution error")

        merged_entities = agnostic_entity._find_merged_entities(entity_uri)

        assert merged_entities == set()

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_find_reverse_related_entities_with_sparql_error(self, mock_sparql_class):
        entity_uri = "https://github.com/arcangelo7/time_agnostic/test/entity"
        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance
        mock_sparql_instance.run_select_query.side_effect = Exception("SPARQL execution error")

        reverse_entities = agnostic_entity._find_reverse_related_entities(entity_uri)

        assert reverse_entities == set()

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_query_dataset_with_non_quadstore(self, mock_sparql_class):
        non_quadstore_config = CONFIG.copy()
        non_quadstore_config["dataset"] = {
            "triplestore_urls": ["http://127.0.0.1:41720/sparql"],
            "file_paths": [],
            "is_quadstore": False
        }

        entity_uri = "https://github.com/arcangelo7/time_agnostic/test/entity"
        agnostic_entity = AgnosticEntity(entity_uri, config=non_quadstore_config)

        mock_sparql_instance = MagicMock()
        mock_sparql_class.return_value = mock_sparql_instance
        mock_sparql_instance.run_select_to_quad_set.return_value = set()

        agnostic_entity._query_dataset(entity_uri)

        mock_sparql_class.assert_called_once()
        call_args = mock_sparql_class.call_args
        query = call_args[0][0]

        assert "GRAPH ?g" not in query
        assert "SELECT ?s ?p ?o" in query
        assert "?g" not in query

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_find_reverse_related_entities_with_non_quadstore(self, mock_sparql_class):
        non_quadstore_config = CONFIG.copy()
        non_quadstore_config["dataset"] = {
            "triplestore_urls": ["http://127.0.0.1:41720/sparql"],
            "file_paths": [],
            "is_quadstore": False
        }

        entity_uri = "https://github.com/arcangelo7/time_agnostic/test/entity"
        agnostic_entity = AgnosticEntity(entity_uri, config=non_quadstore_config)

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

        result = agnostic_entity._find_reverse_related_entities(entity_uri)

        assert result == {'https://example.com/reverse1', 'https://example.com/reverse2'}

        mock_sparql_class.assert_called_once()
        call_args = mock_sparql_class.call_args
        query = call_args[0][0]

        assert "GRAPH" not in query
        assert "SELECT ?subject" in query

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_filter_timestamps_with_missing_time_value(self, mock_sparql_class):
        iterator = [
            {'time': {'value': '2021-05-07T09:59:15.000Z'}},
            {'time': {}},
            {'time': {'value': '2021-06-01T18:46:41.000Z'}},
        ]

        interval = ("2021-05-01T00:00:00+00:00", "2021-06-30T23:59:59+00:00")

        result = _filter_timestamps_by_interval(interval, iterator, time_index='time')

        assert len(result) == 2
        assert result[0]['time']['value'] == '2021-05-07T09:59:15.000Z'
        assert result[1]['time']['value'] == '2021-06-01T18:46:41.000Z'

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_filter_timestamps_with_missing_time_index(self, mock_sparql_class):
        iterator = [
            {'time': {'value': '2021-05-07T09:59:15.000Z'}},
            {'other_key': {'value': '2021-06-01T18:46:41.000Z'}},
        ]

        interval = ("2021-05-01T00:00:00+00:00", "2021-06-30T23:59:59+00:00")

        result = _filter_timestamps_by_interval(interval, iterator, time_index='timestamp')

        assert len(result) == 0

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_filter_timestamps_with_only_after_time(self, mock_sparql_class):
        iterator = [
            {'time': {'value': '2021-05-07T09:59:15.000Z'}},
            {'time': {'value': '2021-06-01T18:46:41.000Z'}},
            {'time': {'value': '2021-07-15T12:00:00.000Z'}},
        ]

        interval = ("2021-06-01T00:00:00+00:00", None)

        result = _filter_timestamps_by_interval(interval, iterator, time_index='time')

        assert len(result) == 2
        assert result[0]['time']['value'] == '2021-06-01T18:46:41.000Z'
        assert result[1]['time']['value'] == '2021-07-15T12:00:00.000Z'

    def test_manage_update_queries_with_malformed_query(self):
        graph = set()

        malformed_query = "THIS IS NOT VALID SPARQL { DELETE SOMETHING }"

        AgnosticEntity._manage_update_queries(graph, malformed_query)

        assert len(graph) == 0

    def test_include_prov_metadata_with_was_derived_from(self):
        entity_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        agnostic_entity = AgnosticEntity(entity_uri, config=CONFIG)

        snapshot1 = "<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1>"
        snapshot2 = "<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2>"
        snapshot3 = "<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3>"
        gen_at_time_n3 = f"<{ProvEntity.iri_generated_at_time}>"
        was_derived_n3 = f"<{ProvEntity.iri_was_derived_from}>"
        time1_n3 = '"2021-05-07T09:59:15.000Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>'

        current_state = {
            (snapshot1, gen_at_time_n3, time1_n3, ""),
            (snapshot2, was_derived_n3, snapshot1, ""),
            (snapshot3, was_derived_n3, snapshot2, ""),
        }

        triples_generated_at_time = [(snapshot1, gen_at_time_n3, time1_n3, "")]

        result = agnostic_entity._include_prov_metadata(triples_generated_at_time, current_state)

        assert isinstance(result, dict)

    def test_fast_parse_update_with_lang_tagged_literal(self):
        query = (
            'DELETE DATA { GRAPH <http://ex.com/g/> { '
            '<http://ex.com/s> <http://ex.com/p> "hello"@en . } }'
        )
        ops = _fast_parse_update(query)
        assert len(ops) == 1
        op_type, quads = ops[0]
        assert op_type == 'DeleteData'
        assert quads[0][2] == '"hello"@en'

    def test_fast_parse_update_with_escaped_literal(self):
        query = (
            'INSERT DATA { GRAPH <http://ex.com/g/> { '
            '<http://ex.com/s> <http://ex.com/p> "line1\\nline2"^^<http://www.w3.org/2001/XMLSchema#string> . } }'
        )
        ops = _fast_parse_update(query)
        assert len(ops) == 1
        assert ops[0][1][0][2] == '"line1\\nline2"^^<http://www.w3.org/2001/XMLSchema#string>'

    def test_find_matching_close_brace_nested(self):
        text = '{ inner { deep } } after'
        pos = _find_matching_close_brace(text, 2)
        assert text[pos] == '}'
        assert text[pos:] == '} after'

    def test_find_matching_close_brace_with_quoted_braces(self):
        text = '{ "contains { brace" } after'
        pos = _find_matching_close_brace(text, 2)
        assert text[pos] == '}'

    def test_find_matching_close_brace_unclosed(self):
        text = '{ no closing brace'
        pos = _find_matching_close_brace(text, 2)
        assert pos == len(text)

    def test_find_matching_close_brace_with_escaped_quote(self):
        text = '{ "escaped \\" quote" } after'
        pos = _find_matching_close_brace(text, 2)
        assert text[pos] == '}'

    def test_find_matching_close_brace_unterminated_string(self):
        text = '{ "unterminated string'
        pos = _find_matching_close_brace(text, 2)
        assert pos == len(text)

    def test_get_state_at_time_with_related_objects_different_timestamps(self):
        entity = AgnosticEntity(
            "https://github.com/arcangelo7/time_agnostic/ar/15519",
            config=CONFIG,
            include_related_objects=True,
        )
        result, _, _ = entity.get_state_at_time(
            time=("2021-05-01T00:00:00", "2021-06-02T00:00:00"),
            include_prov_metadata=False,
        )
        entity_key = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        assert entity_key in result
        ts_2021_05_31 = "2021-05-31T18:19:47+00:00"
        assert ts_2021_05_31 in result[entity_key]
        graph = result[entity_key][ts_2021_05_31]
        subjects = {q[0] for q in graph}
        assert "<https://github.com/arcangelo7/time_agnostic/ra/4>" in subjects

    def test_get_state_at_time_with_none_none_interval(self):
        entity = AgnosticEntity(
            "https://github.com/arcangelo7/time_agnostic/ar/15519",
            config=CONFIG,
        )
        result, _, _ = entity.get_state_at_time(
            time=(None, None),
            include_prov_metadata=False,
        )
        assert len(result) == 3

    def test_iter_versions_nonexistent_entity(self):
        entity = AgnosticEntity(
            "https://github.com/arcangelo7/time_agnostic/nonexistent/999999",
            config=CONFIG,
        )
        versions = list(entity.iter_versions())
        assert versions == []

    def test_compose_update_queries_delete_cancels_add(self):
        queries = [
            'INSERT DATA { GRAPH <http://g/> { <http://s> <http://p> "v" . } }',
            'DELETE DATA { GRAPH <http://g/> { <http://s> <http://p> "v" . } }',
        ]
        additions, deletions = _compose_update_queries(queries)
        assert additions == set()
        assert deletions == set()

    def test_compose_update_queries_insert_cancels_delete(self):
        queries = [
            'DELETE DATA { GRAPH <http://g/> { <http://s> <http://p> "v" . } }',
            'INSERT DATA { GRAPH <http://g/> { <http://s> <http://p> "v" . } }',
        ]
        additions, deletions = _compose_update_queries(queries)
        assert additions == set()
        assert deletions == set()

    def test_find_related_object_uris_none_quad_set(self):
        graphs = {
            'ts1': None,
            'ts2': {('<http://e>', '<http://p>', '<http://o>', '<http://g>')},
        }
        result = _find_related_object_uris('http://e', graphs)
        assert result == {'http://o'}

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_get_history_none_quad_set_replaced(self, mock_sparql_class):
        entity_uri = "https://example.com/entity/1"
        entity = AgnosticEntity(entity_uri, config=CONFIG)
        mock_sparql = MagicMock()
        mock_sparql_class.return_value = mock_sparql
        mock_sparql.run_select_query.return_value = {'results': {'bindings': []}}
        mock_sparql.run_select_to_quad_set.return_value = set()
        entity._get_entity_current_state = MagicMock(return_value=[
            {entity_uri: {'2021-01-01': None}},
            {},
        ])
        entity._get_old_graphs = MagicMock(return_value=[
            {entity_uri: {'2021-01-01': None}},
            {},
        ])
        result = entity.get_history()
        assert result[0][entity_uri]['2021-01-01'] == set()

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_get_delta_entity_exists_before_start(self, mock_sparql_class):
        entity_uri = "https://example.com/entity/1"
        entity = AgnosticEntity(entity_uri, config=CONFIG)
        mock_sparql = MagicMock()
        mock_sparql_class.return_value = mock_sparql
        mock_sparql.run_select_query.return_value = {
            'results': {'bindings': [
                {'time': {'value': '2020-01-01T00:00:00+00:00'}},
                {'time': {'value': '2021-06-01T00:00:00+00:00'}, 'updateQuery': {'value': 'INSERT DATA { GRAPH <http://g/> { <http://s> <http://p> "new" . } }'}},
                {'time': {'value': '2021-08-01T00:00:00+00:00'}, 'updateQuery': {'value': 'INSERT DATA { GRAPH <http://g/> { <http://s> <http://p> "later" . } }'}},
            ]}
        }
        additions, deletions = entity.get_delta('2021-05-01T00:00:00+00:00', '2021-07-01T00:00:00+00:00')
        assert additions == {('<http://s>', '<http://p>', '"new"', '<http://g/>')}
        assert deletions == set()

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_get_delta_entity_created_after_start_no_state_at_end(self, mock_sparql_class):
        entity_uri = "https://example.com/entity/1"
        entity = AgnosticEntity(entity_uri, config=CONFIG)
        mock_sparql = MagicMock()
        mock_sparql_class.return_value = mock_sparql
        mock_sparql.run_select_query.return_value = {
            'results': {'bindings': [
                {'time': {'value': '2021-06-01T00:00:00+00:00'}},
            ]}
        }
        mock_sparql.run_select_to_quad_set.return_value = set()
        entity._get_entity_state_at_time = MagicMock(return_value=({}, {}, {}))
        additions, deletions = entity.get_delta('2021-01-01T00:00:00+00:00', '2021-07-01T00:00:00+00:00')
        assert additions == set()
        assert deletions == set()

    def test_filter_timestamps_by_interval_none(self):
        data = [{'time': {'value': '2021-05-07T09:59:15.000Z'}}]
        result = _filter_timestamps_by_interval(None, data, time_index='time')
        assert result == data

    @patch('time_agnostic_library.agnostic_entity.Sparql')
    def test_get_state_at_time_earlier_snapshot_used(self, mock_sparql_class):
        entity_uri = "https://example.com/entity/1"
        entity = AgnosticEntity(entity_uri, config=CONFIG)
        mock_sparql = MagicMock()
        mock_sparql_class.return_value = mock_sparql

        snap_uri = f"{entity_uri}/prov/se/1"
        gen_time = "2021-03-01T00:00:00+00:00"
        mock_sparql.run_select_query.return_value = {
            'results': {'bindings': [
                {
                    'snapshot': {'value': snap_uri},
                    'time': {'value': gen_time},
                    'responsibleAgent': {'value': 'https://orcid.org/0000-0002-8420-0696'},
                }
            ]}
        }
        mock_sparql.run_select_to_quad_set.return_value = {
            ('<http://s>', '<http://p>', '"val"', '<http://g>'),
        }

        entity_graphs, _, _ = entity._get_entity_state_at_time(
            ("2021-05-01T00:00:00+00:00", "2021-06-01T00:00:00+00:00"), False
        )
        assert gen_time in entity_graphs
