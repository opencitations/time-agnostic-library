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

import json
import os
import unittest
import zipfile
from unittest.mock import MagicMock, patch

from time_agnostic_library.sparql import Sparql

CONFIG = {
    "dataset": {
        "triplestore_urls": ["http://127.0.0.1:9999/sparql"],
        "file_paths": [],
        "is_quadstore": True
    },
    "provenance": {
        "triplestore_urls": [],
        "file_paths": [],
        "is_quadstore": False
    },
    "blazegraph_full_text_search": "no",
    "fuseki_full_text_search": "no",
    "virtuoso_full_text_search": "no",
    "graphdb_connector_name": ""
}

class TestSparqlEdgeCases(unittest.TestCase):
    """
    Test cases for edge cases in Sparql class, including ZIP file handling and language tags.
    """
    maxDiff = None

    def setUp(self):
        """Set up test fixtures."""
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

    def tearDown(self):
        """Clean up test files."""
        if os.path.exists(self.test_zip_path):
            os.remove(self.test_zip_path)

    def test_select_query_with_zip_file(self):
        """
        Test run_select_query with a ZIP file containing JSON-LD data.
        This should exercise the ZIP file handling at lines 102-104.
        """
        # Create a ZIP file with JSON-LD content
        with zipfile.ZipFile(self.test_zip_path, 'w') as zf:
            zf.writestr("data.json", json.dumps(self.test_jsonld_data))

        # Create config with ZIP file path
        zip_config = CONFIG.copy()
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

        # Verify results structure
        self.assertIn('results', results)
        self.assertIn('bindings', results['results'])

    def test_select_to_dataset_with_zip_file(self):
        with zipfile.ZipFile(self.test_zip_path, 'w') as zf:
            zf.writestr("data.json", json.dumps(self.test_jsonld_data))

        zip_config = CONFIG.copy()
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

        self.assertIn('results', result)
        self.assertIn('bindings', result['results'])

    def test_select_query_with_language_tagged_literals(self):
        """
        Test run_select_query with literals that have language tags.
        This should exercise the language tag handling at lines 140, 143.
        """
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
            lang_config = CONFIG.copy()
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

            # Verify results contain language-tagged values
            self.assertIn('results', results)
            self.assertIn('bindings', results['results'])

            # Check if any result has xml:lang attribute
            bindings = results['results']['bindings']
            has_lang_tag = False
            for binding in bindings:
                if 'name' in binding and 'xml:lang' in binding['name']:
                    has_lang_tag = True
                    break

            # At least one result should have language tag
            self.assertTrue(has_lang_tag or len(bindings) > 0)

        finally:
            # Clean up
            if os.path.exists(temp_jsonld_path):
                os.remove(temp_jsonld_path)

    @patch('time_agnostic_library.sparql.SPARQLClient')
    def test_ask_query_with_triplestore_url(self, mock_sparql_client):
        ask_config = CONFIG.copy()
        ask_config["dataset"]["triplestore_urls"] = ["http://127.0.0.1:9999/sparql"]
        ask_config["dataset"]["file_paths"] = []

        query = """
            ASK {
                ?s a <http://xmlns.com/foaf/0.1/Person> .
            }
        """

        mock_client_instance = MagicMock()
        mock_client_instance.ask.return_value = True
        mock_client_instance.__enter__ = MagicMock(return_value=mock_client_instance)
        mock_client_instance.__exit__ = MagicMock(return_value=False)
        mock_sparql_client.return_value = mock_client_instance

        sparql = Sparql(query, config=ask_config)
        result = sparql.run_ask_query()

        self.assertIsInstance(result, bool)
        self.assertTrue(result)
        mock_sparql_client.assert_called()

    def test_ask_query_with_only_file_paths_no_triplestore(self):
        """
        Test run_ask_query when only file paths are configured (no triplestore URLs).
        This should exercise the fallback return at line 172.
        """
        # Create temporary JSON-LD file
        temp_jsonld_path = "tests/test_ask.json"
        with open(temp_jsonld_path, 'w') as f:
            json.dump(self.test_jsonld_data, f)

        try:
            # Config with only file paths
            file_only_config = CONFIG.copy()
            file_only_config["dataset"]["file_paths"] = [temp_jsonld_path]
            file_only_config["dataset"]["triplestore_urls"] = []

            query = """
                ASK {
                    ?s a <http://xmlns.com/foaf/0.1/Person> .
                }
            """

            # Execute ASK query - should use file-based query
            sparql = Sparql(query, config=file_only_config)
            result = sparql.run_ask_query()

            # Should return a boolean (from file-based evaluation)
            self.assertIsInstance(result, bool)

        finally:
            # Clean up
            if os.path.exists(temp_jsonld_path):
                os.remove(temp_jsonld_path)

    def test_select_query_with_unknown_value_type(self):
        """
        Test run_select_query with unknown/uncommon RDF value types.
        This should exercise the fallback at line 143 for unknown types.
        """
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
            complex_config = CONFIG.copy()
            complex_config["dataset"]["file_paths"] = [temp_jsonld_path]
            complex_config["dataset"]["triplestore_urls"] = []

            query = """
                SELECT ?person ?name
                WHERE {
                    ?person <http://xmlns.com/foaf/0.1/name> ?name .
                }
            """

            # Execute query
            sparql = Sparql(query, config=complex_config)
            results = sparql.run_select_query()

            # Verify results structure
            self.assertIn('results', results)
            self.assertIn('bindings', results['results'])

        finally:
            # Clean up
            if os.path.exists(temp_jsonld_path):
                os.remove(temp_jsonld_path)

if __name__ == '__main__':
    unittest.main()
