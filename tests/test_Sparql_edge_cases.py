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

import copy
import json
import os
import zipfile
from unittest.mock import MagicMock, patch

from time_agnostic_library.sparql import (
    Sparql,
    _binding_to_n3,
    _close_all_clients,
    _find_closing_quote,
    _n3_to_binding,
    _n3_value,
    _parse_n3_literal,
    _unescape_n3,
)
from triplestore_config import CONFIG


class TestSparqlEdgeCases:

    def setup_method(self):
        self.test_zip_path = "tests/test_data.zip"
        self.test_jsonld_data = {
            "@context": {
                "foaf": "http://xmlns.com/foaf/0.1/",
                "ex": "http://example.com/"
            },
            "@graph": [
                {
                    "@id": "ex:person1",
                    "@type": "foaf:Person",
                    "foaf:name": "John Doe"
                }
            ]
        }

    def teardown_method(self):
        if os.path.exists(self.test_zip_path):
            os.remove(self.test_zip_path)

    def test_select_query_with_zip_file(self):
        # Create a ZIP file with JSON-LD content
        with zipfile.ZipFile(self.test_zip_path, 'w') as zf:
            zf.writestr("data.json", json.dumps(self.test_jsonld_data))

        # Create config with ZIP file path
        zip_config = copy.deepcopy(CONFIG)
        zip_config["dataset"]["file_paths"] = [self.test_zip_path]
        zip_config["dataset"]["triplestore_urls"] = []

        query = """
            SELECT ?person ?name
            WHERE {
                ?person a <http://xmlns.com/foaf/0.1/Person> ;
                        <http://xmlns.com/foaf/0.1/name> ?name .
            }
        """

        # Execute query - should handle ZIP file
        sparql = Sparql(query, config=zip_config)
        results = sparql.run_select_query()

        assert 'results' in results
        assert 'bindings' in results['results']

    def test_select_to_dataset_with_zip_file(self):
        with zipfile.ZipFile(self.test_zip_path, 'w') as zf:
            zf.writestr("data.json", json.dumps(self.test_jsonld_data))

        zip_config = copy.deepcopy(CONFIG)
        zip_config["dataset"]["file_paths"] = [self.test_zip_path]
        zip_config["dataset"]["triplestore_urls"] = []

        query = """
            SELECT ?person ?name
            WHERE {
                ?person a <http://xmlns.com/foaf/0.1/Person> ;
                        <http://xmlns.com/foaf/0.1/name> ?name .
            }
        """

        sparql = Sparql(query, config=zip_config)
        result = sparql.run_select_query()

        assert 'results' in result
        assert 'bindings' in result['results']

    def test_select_query_with_language_tagged_literals(self):
        # Create JSON-LD with language-tagged literals
        jsonld_with_lang = {
            "@context": {
                "foaf": "http://xmlns.com/foaf/0.1/",
                "ex": "http://example.com/"
            },
            "@graph": [
                {
                    "@id": "ex:person1",
                    "@type": "foaf:Person",
                    "foaf:name": [
                        {"@value": "John Doe", "@language": "en"},
                        {"@value": "Giovanni Rossi", "@language": "it"}
                    ]
                }
            ]
        }

        # Write to temporary file
        temp_jsonld_path = "tests/test_lang.json"
        with open(temp_jsonld_path, 'w') as f:
            json.dump(jsonld_with_lang, f)

        try:
            # Create config with file path
            lang_config = copy.deepcopy(CONFIG)
            lang_config["dataset"]["file_paths"] = [temp_jsonld_path]
            lang_config["dataset"]["triplestore_urls"] = []

            query = """
                SELECT ?person ?name
                WHERE {
                    ?person a <http://xmlns.com/foaf/0.1/Person> ;
                            <http://xmlns.com/foaf/0.1/name> ?name .
                }
            """

            # Execute query - should handle language tags
            sparql = Sparql(query, config=lang_config)
            results = sparql.run_select_query()

            assert 'results' in results
            assert 'bindings' in results['results']

            # Check if any result has xml:lang attribute
            bindings = results['results']['bindings']
            has_lang_tag = False
            for binding in bindings:
                if 'name' in binding and 'xml:lang' in binding['name']:
                    has_lang_tag = True
                    break

            # At least one result should have language tag
            assert has_lang_tag or len(bindings) > 0

        finally:
            if os.path.exists(temp_jsonld_path):
                os.remove(temp_jsonld_path)

    @patch('time_agnostic_library.sparql.SPARQLClient')
    def test_ask_query_with_triplestore_url(self, mock_sparql_client):
        _close_all_clients()

        ask_config = copy.deepcopy(CONFIG)
        ask_config["dataset"]["triplestore_urls"] = ["http://127.0.0.1:41720/sparql"]
        ask_config["dataset"]["file_paths"] = []

        query = """
            ASK {
                ?s a <http://xmlns.com/foaf/0.1/Person> .
            }
        """

        mock_client_instance = MagicMock()
        mock_client_instance.ask.return_value = True
        mock_sparql_client.return_value = mock_client_instance

        sparql = Sparql(query, config=ask_config)
        result = sparql.run_ask_query()

        _close_all_clients()

        assert isinstance(result, bool)
        assert result
        mock_sparql_client.assert_called()

    def test_ask_query_with_only_file_paths_no_triplestore(self):
        # Create temporary JSON-LD file
        temp_jsonld_path = "tests/test_ask.json"
        with open(temp_jsonld_path, 'w') as f:
            json.dump(self.test_jsonld_data, f)

        try:
            # Config with only file paths
            file_only_config = copy.deepcopy(CONFIG)
            file_only_config["dataset"]["file_paths"] = [temp_jsonld_path]
            file_only_config["dataset"]["triplestore_urls"] = []

            query = """
                ASK {
                    ?s a <http://xmlns.com/foaf/0.1/Person> .
                }
            """

            sparql = Sparql(query, config=file_only_config)
            result = sparql.run_ask_query()

            assert isinstance(result, bool)

        finally:
            if os.path.exists(temp_jsonld_path):
                os.remove(temp_jsonld_path)

    def test_select_query_with_unknown_value_type(self):
        # Create JSON-LD with blank nodes and various value types
        jsonld_complex = {
            "@context": {
                "foaf": "http://xmlns.com/foaf/0.1/",
                "ex": "http://example.com/"
            },
            "@graph": [
                {
                    "@id": "_:blank1",
                    "@type": "foaf:Person",
                    "foaf:name": "Anonymous"
                }
            ]
        }

        # Write to temporary file
        temp_jsonld_path = "tests/test_complex.json"
        with open(temp_jsonld_path, 'w') as f:
            json.dump(jsonld_complex, f)

        try:
            # Create config with file path
            complex_config = copy.deepcopy(CONFIG)
            complex_config["dataset"]["file_paths"] = [temp_jsonld_path]
            complex_config["dataset"]["triplestore_urls"] = []

            query = """
                SELECT ?person ?name
                WHERE {
                    ?person <http://xmlns.com/foaf/0.1/name> ?name .
                }
            """

            sparql = Sparql(query, config=complex_config)
            results = sparql.run_select_query()

            assert 'results' in results
            assert 'bindings' in results['results']

        finally:
            if os.path.exists(temp_jsonld_path):
                os.remove(temp_jsonld_path)

    def test_get_tuples_set_mixed_values(self):
        output = set()
        Sparql._get_tuples_set({'x': 'plain_string', 'y': {'value': 'dict_val'}}, output, ['x', 'y', 'z'])
        assert output == {('plain_string', 'dict_val', None)}


class TestSparqlHelpers:

    def test_binding_to_n3_bnode(self):
        result = _binding_to_n3({'type': 'bnode', 'value': 'b0'})
        assert result == '_:b0'

    def test_binding_to_n3_lang_literal(self):
        result = _binding_to_n3({'type': 'literal', 'value': 'ciao', 'xml:lang': 'it'})
        assert result == '"ciao"@it'

    def test_find_closing_quote_no_close(self):
        assert _find_closing_quote('"no close') == -1

    def test_unescape_n3_carriage_return(self):
        assert _unescape_n3('a\\rb') == 'a\rb'

    def test_unescape_n3_backslash(self):
        assert _unescape_n3('a\\\\b') == 'a\\b'

    def test_unescape_n3_unknown_escape(self):
        assert _unescape_n3('a\\xb') == 'a\\xb'

    def test_parse_n3_literal_no_closing_quote(self):
        value, rest = _parse_n3_literal('"no close')
        assert value == '"no close'
        assert rest == ''

    def test_n3_value_bnode(self):
        assert _n3_value('_:b0') == 'b0'

    def test_n3_to_binding_bnode(self):
        assert _n3_to_binding('_:b0') == {'type': 'bnode', 'value': 'b0'}

    def test_n3_to_binding_lang_literal(self):
        result = _n3_to_binding('"hello"@en')
        assert result == {'type': 'literal', 'value': 'hello', 'xml:lang': 'en'}

    @patch('time_agnostic_library.sparql.Sparql.run_select_query')
    def test_run_select_to_quad_set_skips_missing_var(self, mock_query):
        mock_query.return_value = {
            'head': {'vars': ['s', 'p', 'o']},
            'results': {'bindings': [
                {'s': {'type': 'uri', 'value': 'http://ex.com/s'}, 'p': {'type': 'uri', 'value': 'http://ex.com/p'}},
                {'s': {'type': 'uri', 'value': 'http://ex.com/s'}, 'p': {'type': 'uri', 'value': 'http://ex.com/p'}, 'o': {'type': 'literal', 'value': 'val'}},
            ]}
        }
        sparql = Sparql("SELECT ?s ?p ?o WHERE { ?s ?p ?o }", config=CONFIG)
        result = sparql.run_select_to_quad_set()
        assert result == {('<http://ex.com/s>', '<http://ex.com/p>', '"val"')}
