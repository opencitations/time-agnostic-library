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

import pytest
from unittest.mock import MagicMock, patch

from triplestore_config import CONFIG

from time_agnostic_library.agnostic_query import (
    DeltaQuery,
    VersionQuery,
    _build_delta_result,
    _match_single_pattern,
    _reconstruct_at_time_as_sets,
    get_insert_query,
)


class TestAgnosticQueryEdgeCases:

    def test_fuseki_full_text_search_configuration(self):
        fuseki_config = CONFIG.copy()
        fuseki_config["fuseki_full_text_search"] = "yes"

        query = """
            SELECT ?agent
            WHERE {
                ?agent a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        version_query = VersionQuery(query, config_dict=fuseki_config)

        assert version_query is not None

    def test_virtuoso_full_text_search_configuration(self):
        virtuoso_config = CONFIG.copy()
        virtuoso_config["virtuoso_full_text_search"] = "yes"

        query = """
            SELECT ?agent
            WHERE {
                ?agent a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        version_query = VersionQuery(query, config_dict=virtuoso_config)

        assert version_query is not None

    def test_graphdb_connector_configuration(self):
        graphdb_config = CONFIG.copy()
        graphdb_config["graphdb_connector_name"] = "test_connector"

        query = """
            SELECT ?agent
            WHERE {
                ?agent a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        version_query = VersionQuery(query, config_dict=graphdb_config)

        assert version_query is not None

    def test_configuration_with_invalid_full_text_search_value(self):
        invalid_config = CONFIG.copy()
        invalid_config["blazegraph_full_text_search"] = "invalid_value"

        query = """
            SELECT ?agent
            WHERE {
                ?agent a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        with pytest.raises(ValueError) as context:
            VersionQuery(query, config_dict=invalid_config)

        assert "full_text_search" in str(context.value).lower()

    def test_configuration_with_multiple_full_text_search_enabled(self):
        multi_config = CONFIG.copy()
        multi_config["blazegraph_full_text_search"] = "yes"
        multi_config["fuseki_full_text_search"] = "yes"

        query = """
            SELECT ?agent
            WHERE {
                ?agent a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        with pytest.raises(ValueError) as context:
            VersionQuery(query, config_dict=multi_config)

        assert "multiple indexing systems" in str(context.value).lower()

    def test_query_with_no_hooks(self):
        query_no_hooks = """
            SELECT *
            WHERE { }
        """

        version_query = VersionQuery(query_no_hooks, config_dict=CONFIG)
        assert version_query is not None

    def test_version_query_with_config_dict_parameter(self):
        query = """
            SELECT ?agent
            WHERE {
                ?agent a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        version_query = VersionQuery(query, config_dict=CONFIG)

        assert version_query is not None
        assert version_query.config == CONFIG

    def test_get_insert_query_with_empty_set(self):
        result, num_statements = get_insert_query("http://example.com/graph", set())
        assert result == ""
        assert num_statements == 0

    def test_get_insert_query_with_non_empty_set(self):
        data = {('<http://example.com/s>', '<http://example.com/p>', '"object"')}
        result, num_statements = get_insert_query("http://example.com/graph", data)
        assert isinstance(result, str)
        assert "INSERT" in result.upper()
        assert num_statements == 1

    @patch('time_agnostic_library.agnostic_query.Sparql')
    def test_update_query_parsing_error(self, mock_sparql_class):
        query = """
            SELECT ?entity
            WHERE {
                ?entity a <http://xmlns.com/foaf/0.1/Agent> .
            }
        """

        DeltaQuery(query, config_dict=CONFIG, changed_properties=set())

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

    def test_reconstruct_at_time_as_sets_empty_prov(self):
        result = _reconstruct_at_time_as_sets([], set(), ("2021-05-20T00:00:00+00:00", "2021-05-20T00:00:00+00:00"))
        assert result == []

    def test_reconstruct_at_time_as_sets_earlier_snapshot(self):
        prov = [
            {'time': '2021-05-07T09:59:15+00:00', 'updateQuery': None},
            {'time': '2021-06-01T18:46:41+00:00', 'updateQuery': None},
        ]
        quads = {('<http://s>', '<http://p>', '"o"', '<http://g>')}
        result = _reconstruct_at_time_as_sets(prov, quads, ("2021-05-20T00:00:00+00:00", "2021-05-20T00:00:00+00:00"))
        assert len(result) == 1
        assert result[0][0] == "2021-05-07T09:59:15+00:00"

    def test_reconstruct_at_time_as_sets_no_earlier_snapshot(self):
        prov = [
            {'time': '2021-05-07T09:59:15+00:00', 'updateQuery': None},
        ]
        quads = {('<http://s>', '<http://p>', '"o"', '<http://g>')}
        result = _reconstruct_at_time_as_sets(prov, quads, ("2021-01-01T00:00:00+00:00", "2021-01-01T00:00:00+00:00"))
        assert result == []

    def test_reconstruct_at_time_as_sets_no_interval_start(self):
        prov = [
            {'time': '2021-05-07T09:59:15+00:00', 'updateQuery': None},
        ]
        quads = {('<http://s>', '<http://p>', '"o"', '<http://g>')}
        result = _reconstruct_at_time_as_sets(prov, quads, (None, "2021-01-01T00:00:00+00:00"))
        assert result == []

    def test_generic_triple_pattern_raises_error(self):
        query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
        with pytest.raises(ValueError) as ctx:
            VersionQuery(query, config_dict=CONFIG)
        assert "specify at least one URI" in str(ctx.value)

    def test_match_single_pattern_subject_filter(self):
        quads = (
            ('<http://a>', '<http://p>', '"val"', '<http://g>'),
            ('<http://b>', '<http://p>', '"val"', '<http://g>'),
        )
        result = _match_single_pattern(('<http://a>', '<http://p>', '?o'), quads)
        assert len(result) == 1
        assert result[0]['o']['value'] == 'val'

    def test_collect_triples_flat_with_nested_optional(self):
        query = "SELECT ?s ?o ?v WHERE { ?s <http://ex.com/p> <http://ex.com/o> OPTIONAL { ?s <http://ex.com/q> ?o OPTIONAL { ?o <http://ex.com/r> ?v } } }"
        vq = VersionQuery(query, config_dict=CONFIG)
        assert vq.triples is not None

    @patch('time_agnostic_library.agnostic_query.Sparql')
    def test_get_present_entities_reverse_with_var_object(self, mock_sparql_class):
        query = "SELECT ?s WHERE { ?s <http://ex.com/p> <http://ex.com/o> }"
        vq = VersionQuery(query, config_dict=CONFIG)
        mock_sparql = MagicMock()
        mock_sparql_class.return_value = mock_sparql
        mock_sparql.run_select_query.return_value = {
            'results': {'bindings': [
                {'o': {'type': 'uri', 'value': 'http://ex.com/result'}},
            ]}
        }
        result = vq._get_present_entities(('?s', '^<http://ex.com/p>', '?o'))
        assert result == {'http://ex.com/result'}

    def test_build_delta_result_break_after_before_dt(self):
        snapshots = [
            {'time': '2021-01-01T00:00:00+00:00', 'updateQuery': None, 'invalidatedAtTime': None},
            {'time': '2021-06-01T00:00:00+00:00', 'updateQuery': 'INSERT DATA { GRAPH <http://g/> { <http://s> <http://p> "v" . } }', 'invalidatedAtTime': None},
            {'time': '2021-12-01T00:00:00+00:00', 'updateQuery': 'INSERT DATA { GRAPH <http://g/> { <http://s> <http://p> "late" . } }', 'invalidatedAtTime': None},
        ]
        result = _build_delta_result('http://ex.com/e', snapshots, ('2021-01-01T00:00:00+00:00', '2021-07-01T00:00:00+00:00'), set())
        assert 'http://ex.com/e' in result
        assert result['http://ex.com/e']['additions'] == {('<http://s>', '<http://p>', '"v"', '<http://g/>')}

    @patch('time_agnostic_library.agnostic_query.Sparql')
    def test_find_entity_uris_in_update_queries_with_full_text_search(self, mock_sparql_class):
        query = "SELECT ?s WHERE { ?s <http://ex.com/p> <http://ex.com/o> }"
        config_fts = CONFIG.copy()
        config_fts["blazegraph_full_text_search"] = "yes"
        vq = VersionQuery(query, config_dict=config_fts)
        mock_sparql = MagicMock()
        mock_sparql_class.return_value = mock_sparql
        mock_sparql.run_select_query.return_value = {
            'results': {'bindings': [
                {
                    'updateQuery': {'value': 'INSERT DATA { GRAPH <http://g/> { <http://ex.com/e1> <http://ex.com/p> <http://ex.com/o> . } }'},
                },
            ]}
        }
        entities = set()
        triple = ('?s', '<http://ex.com/p>', '<http://ex.com/o>')
        vq._find_entity_uris_in_update_queries(triple, entities)
        assert entities == {'http://ex.com/e1'}

    @patch('time_agnostic_library.agnostic_query.Sparql')
    def test_find_entities_in_update_queries_default_none(self, mock_sparql_class):
        query = "SELECT ?s WHERE { ?s <http://ex.com/p> <http://ex.com/o> }"
        config_fts = CONFIG.copy()
        config_fts["blazegraph_full_text_search"] = "yes"
        vq = VersionQuery(query, config_dict=config_fts)
        mock_sparql = MagicMock()
        mock_sparql_class.return_value = mock_sparql
        mock_sparql.run_select_query.return_value = {'results': {'bindings': []}}
        triple = ('?s', '<http://ex.com/p>', '<http://ex.com/o>')
        vq._find_entities_in_update_queries(triple)
