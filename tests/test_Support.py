#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022, Arcangelo Massari <arcangelo.massari@unibo.it>
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
import unittest
from datetime import datetime, timezone

from SPARQLWrapper import JSON, SPARQLWrapper
from time_agnostic_library.support import (convert_to_datetime,
                                           generate_config_file)

CONFIG_PATH = "tests/config_support_test.json"
CONFIG_GRAPHDB = "tests/config_graphdb.json"
CONFIG_FUSEKI= "tests/config_fuseki.json"
CONFIG_VIRTUOSO= "tests/config_virtuoso.json"

class Test_Support(unittest.TestCase):
    def setUp(self):
        self.config_path = CONFIG_PATH
        # Create the configuration file with the exact required content
        self.initial_config = {
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
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(self.initial_config, f)

    def tearDown(self):
        try:
            import os
            if os.path.exists(self.config_path):
                os.remove(self.config_path)
        except Exception as e:
            print(f"Error during cleanup: {e}")

    def test_convert_to_datetime(self):
        input = "2021-05-21T19:08:56+00:00"
        expected_output = datetime(2021, 5, 21, 19, 8, 56, tzinfo=timezone.utc)
        self.assertEqual(convert_to_datetime(input), expected_output)

    def test_generate_config_file(self):
        # Test case 1: Basic configuration with default values
        expected_config = {
            'dataset': {
                'triplestore_urls': [],
                'file_paths': [],
                'is_quadstore': True
            },
            'provenance': {
                'triplestore_urls': [],
                'file_paths': [],
                'is_quadstore': True
            },
            'blazegraph_full_text_search': 'false',
            'fuseki_full_text_search': 'false',
            'virtuoso_full_text_search': 'false',
            'graphdb_connector_name': ''
        }
        config = generate_config_file(self.config_path)
        with open(self.config_path, encoding='utf-8') as f:
            generated_config = json.load(f)
        self.assertEqual(config, expected_config)
        self.assertEqual(generated_config, expected_config)

        # Test case 2: Configuration with custom values
        test_dataset_urls = ['http://example.com/dataset']
        test_provenance_urls = ['http://example.com/provenance']
        expected_config_custom = {
            'dataset': {
                'triplestore_urls': test_dataset_urls,
                'file_paths': [],
                'is_quadstore': False
            },
            'provenance': {
                'triplestore_urls': test_provenance_urls,
                'file_paths': [],
                'is_quadstore': True
            },
            'blazegraph_full_text_search': 'true',
            'fuseki_full_text_search': 'false',
            'virtuoso_full_text_search': 'true',
            'graphdb_connector_name': 'test_connector'
        }
        config = generate_config_file(
            config_path=self.config_path,
            dataset_urls=test_dataset_urls,
            dataset_is_quadstore=False,
            provenance_urls=test_provenance_urls,
            blazegraph_full_text_search=True,
            virtuoso_full_text_search=True,
            graphdb_connector_name='test_connector'
        )
        with open(self.config_path, encoding='utf-8') as f:
            generated_config = json.load(f)
        self.assertEqual(config, expected_config_custom)
        self.assertEqual(generated_config, expected_config_custom)

        # Test case 3: Test with file paths
        test_dataset_dirs = ['path/to/dataset']
        test_provenance_dirs = ['path/to/provenance']
        expected_config_paths = {
            'dataset': {
                'triplestore_urls': [],
                'file_paths': test_dataset_dirs,
                'is_quadstore': True
            },
            'provenance': {
                'triplestore_urls': [],
                'file_paths': test_provenance_dirs,
                'is_quadstore': False
            },
            'blazegraph_full_text_search': 'false',
            'fuseki_full_text_search': 'true',
            'virtuoso_full_text_search': 'false',
            'graphdb_connector_name': ''
        }
        config = generate_config_file(
            config_path=self.config_path,
            dataset_dirs=test_dataset_dirs,
            provenance_dirs=test_provenance_dirs,
            provenance_is_quadstore=False,
            fuseki_full_text_search=True
        )
        with open(self.config_path, encoding='utf-8') as f:
            generated_config = json.load(f)
        self.assertEqual(config, expected_config_paths)
        self.assertEqual(generated_config, expected_config_paths)

if __name__ == '__main__': # pragma: no cover
    unittest.main()