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

import unittest
from time_agnostic_library.agnostic_query import DeltaQuery

CONFIG_PATH = "tests/config.json"
CONFIG_VIRTUOSO = "tests/config_virtuoso.json"

class Test_DeltaQuery(unittest.TestCase):
    def test_run_agnostic_query_cross_delta(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime. 
            }
        """
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}   
        delta_query = DeltaQuery(query=query, changed_properties=changed_properties, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
                "created": "2021-05-07T09:59:15+00:00",
                "modified": {
                    '2021-06-01T18:46:41+00:00': 'DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }'
                },
                "deleted": None
            }
        }
        self.assertEqual(agnostic_results, expected_output)

    def test_run_agnostic_query_single_delta_before(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime. 
            }
        """
        on_time = (None, "2021-06-02T18:46:41+00:00")
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}   
        delta_query = DeltaQuery(query=query, on_time=on_time, changed_properties=changed_properties, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
                "created": "2021-05-07T09:59:15+00:00",
                "modified": {
                    '2021-06-01T18:46:41+00:00': 'DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }'
                },
                "deleted": None
            }
        }
        self.assertEqual(agnostic_results, expected_output)

    def test_run_agnostic_query_single_delta_interval(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime. 
            }
        """
        on_time = ("2021-06-01T00:00:00+00:00", "2021-06-02T18:46:41+00:00")
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}   
        delta_query = DeltaQuery(query=query, on_time=on_time, changed_properties=changed_properties, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
                "created": None,
                "modified": {
                    '2021-06-01T18:46:41+00:00': 'DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }'
                },
                "deleted": None
            }
        }
        self.assertEqual(agnostic_results, expected_output)

    def test_run_agnostic_query_single_delta_after_no_results(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime. 
            }
        """
        on_time = ("2021-06-02T00:00:00+00:00", None)
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}   
        delta_query = DeltaQuery(query=query, on_time=on_time, changed_properties=changed_properties, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = dict()
        self.assertEqual(agnostic_results, expected_output)

    def test_run_agnostic_query_on_deleted_entity(self):
        query = """
            prefix foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?ra
            WHERE {
                ?ra a foaf:Agent. 
            }
        """
        delta_query = DeltaQuery(query=query, config_path=CONFIG_VIRTUOSO)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ra/15519': {
                'created': '2021-05-07T09:59:15+00:00',
                'deleted': '2021-06-01T18:46:41+00:00',
                'modified': {
                    '2021-06-01T18:46:41+00:00': 'DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ra/> { <https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"^^<http://www.w3.org/2001/XMLSchema#string> .\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"^^<http://www.w3.org/2001/XMLSchema#string> .\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509> .\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"^^<http://www.w3.org/2001/XMLSchema#string> .\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent> .} }'
                }   
            },
            'https://github.com/arcangelo7/time_agnostic/ra/4': {
                'created': '2021-05-07T09:59:15+00:00',
                'deleted': None,
                'modified': {
                    '2021-06-01T18:46:41+00:00': "The entity 'https://github.com/arcangelo7/time_agnostic/ra/4' has been merged with 'https://github.com/arcangelo7/time_agnostic/ra/15519'."
                }
            }      
        }
        self.assertEqual(agnostic_results, expected_output)

if __name__ == '__main__': # pragma: no cover
    unittest.main()