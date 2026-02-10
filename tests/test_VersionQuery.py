#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022-2025, Arcangelo Massari <arcangelo.massari@unibo.it>
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
from unittest.mock import patch

from rdflib import URIRef, Variable
from time_agnostic_library.agnostic_query import VersionQuery
from time_agnostic_library.support import (_to_dict_of_datasets,
                                           _to_dict_of_nt_sorted_lists)

CONFIG_PATH = os.path.join('tests', 'config.json')
CONFIG_VIRTUOSO = os.path.join('tests', 'config_virtuoso.json')
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

def _sort_bindings(bindings):
    return sorted(bindings, key=lambda b: json.dumps(b, sort_keys=True))


class Test_VersionQuery(unittest.TestCase):
    maxDiff = None

    def test__tree_traverse_no_options(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/2> pro:isHeldBy ?o;
            }
        """
        input1 = {
            '_vars': {
                Variable('o')
            },
            'triples': [
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/2'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o'))
            ]
        }
        input2 = "triples"
        output = list()
        VersionQuery(query, config_path=CONFIG_PATH)._tree_traverse(input1, input2, output)
        expected_output = [(URIRef('https://github.com/arcangelo7/time_agnostic/ar/2'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o'))]
        self.assertEqual(output, expected_output)

    def test__tree_traverse_one_option(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/2> pro:isHeldBy ?o;
                    rdf:type pro:RoleInTime.
            }
        """
        input1 = {
            '_vars': {
                Variable('o')},
            'triples': [
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/2'), URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),URIRef('http://purl.org/spar/pro/RoleInTime')),
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/2'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o'))
            ]
        }
        input2 = "triples"
        output = list()
        VersionQuery(query, config_path=CONFIG_PATH)._tree_traverse(input1, input2, output)
        expected_output = [
            (URIRef('https://github.com/arcangelo7/time_agnostic/ar/2'), URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), URIRef('http://purl.org/spar/pro/RoleInTime')), 
            (URIRef('https://github.com/arcangelo7/time_agnostic/ar/2'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o'))
        ]
        self.assertEqual(output, expected_output)

    def test__tree_traverse_more_options(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                OPTIONAL {?id literal:hasLiteralValue ?value.}
            }
        """
        input1 = {
            '_vars': {
                Variable('id'),
                Variable('o'),
                Variable('value')
            },
            'expr': {
                '_vars': set()
            },
            'p1': {
                '_vars': {
                    Variable('o'), 
                    Variable('id')
                },
                'lazy': True,
                'p1': {
                    '_vars': {Variable('o')},
                    'expr': {'_vars': set()},
                    'p1': {'_vars': {Variable('o')},
                    'triples': [(URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o'))]},
                    'p2': {'_vars': set(),
                    'triples': [(URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), URIRef('http://purl.org/spar/pro/RoleInTime'))]}
                },
                'p2': {
                    '_vars': {Variable('o'), Variable('id')},
                    'triples': [(Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id'))]
                }
            },
            'p2': {
                '_vars': {Variable('id'), Variable('value')},
                    'triples': [(Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value'))]
            }
        }
        input2 = "triples"
        output = list()
        VersionQuery(query, config_path=CONFIG_PATH)._tree_traverse(input1, input2, output)
        expected_output = [
            (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
            (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), URIRef('http://purl.org/spar/pro/RoleInTime')), 
            (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')), 
            (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value'))
        ]
        self.assertEqual(output, expected_output)

    def test__process_query(self):
        input = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                OPTIONAL {?id literal:hasLiteralValue ?value.}
            }
        """
        output = VersionQuery(input, config_path=CONFIG_PATH)._process_query()
        expected_output = [
            (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
            (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), URIRef('http://purl.org/spar/pro/RoleInTime')), 
            (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')), 
            (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value'))
        ]
        self.assertEqual(output, expected_output)

    def test__process_query_valueError(self):
        input = """
            CONSTRUCT {<https://github.com/arcangelo7/time_agnostic/ar/15519> ?p ?o}
            WHERE {<https://github.com/arcangelo7/time_agnostic/ar/15519> ?p ?o}
        """
        with self.assertRaises(ValueError):
            VersionQuery(input)._process_query()

    def test__align_snapshots(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        agnostic_query.relevant_entities_graphs = _to_dict_of_datasets({
            URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-31T18:19:47+00:00': 
                [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ]
            }
        })
        agnostic_query.relevant_graphs = dict()
        agnostic_query._align_snapshots()
        expected_output = {
            '2021-06-01T18:46:41+00:00': [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
            ], 
            '2021-05-31T18:19:47+00:00': [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
            ], 
            '2021-05-07T09:59:15+00:00': [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
            ]
        }
        self.assertEqual(_to_dict_of_nt_sorted_lists(agnostic_query.relevant_graphs), expected_output)

    def test__align_snapshots_non_overlapping(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        agnostic_query.relevant_entities_graphs = _to_dict_of_datasets({
            URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-31T18:19:47+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ]
            }, 
            URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'): {
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ], 
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ]
            }, 
            URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'): {
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ], 
                '2021-06-01T18:46:41+00:00': []
            }, 
            URIRef('https://github.com/arcangelo7/time_agnostic/id/14'): {
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>'
                ], 
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>'
                ]
            }, 
            URIRef('https://github.com/arcangelo7/time_agnostic/id/85509'): {
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>'
                ], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>'
                ]
            }
        })
        agnostic_query.relevant_graphs = dict()
        agnostic_query._align_snapshots()
        expected_output = {
            '2021-06-01T18:46:41+00:00': [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>', 
                '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>', 
                '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
            ], 
            '2021-05-31T18:19:47+00:00': [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>', 
                '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>', 
                '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
            ], 
            '2021-05-07T09:59:15+00:00': [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>', 
                '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>', 
                '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
            ]
        }
        self.assertEqual(_to_dict_of_nt_sorted_lists(agnostic_query.relevant_graphs), expected_output)

    def test___rebuild_relevant_entity(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        agnostic_query.relevant_entities_graphs = _to_dict_of_datasets({
            URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
                '2021-05-31T18:19:47+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ]
            }
        })
        entity = URIRef("https://github.com/arcangelo7/time_agnostic/ra/15519")
        agnostic_query.reconstructed_entities = {URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519')}
        agnostic_query._rebuild_relevant_entity(entity)
        expected_relevant_entities_graphs = {
            URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
                '2021-05-31T18:19:47+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ]
            },
            URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'): {
                '2021-06-01T18:46:41+00:00': [], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ]
            }
        }
        expected_reconstructed_entities = {URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'), URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519')}
        assert (agnostic_query.reconstructed_entities, _to_dict_of_nt_sorted_lists(agnostic_query.relevant_entities_graphs)) == (expected_reconstructed_entities, expected_relevant_entities_graphs)

    def test__solve_variables(self):
        query = """
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o ?id
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o.
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        agnostic_query.reconstructed_entities = {URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519')}
        agnostic_query.relevant_entities_graphs = _to_dict_of_datasets({
            URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-31T18:19:47+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ]
            }
        })
        expected_relevant_entities_graphs = {
            URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-31T18:19:47+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ]
            }, 
            URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'): {
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ]
            }, 
            URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'): {
                '2021-06-01T18:46:41+00:00': [], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ]
            }, 
            URIRef('https://github.com/arcangelo7/time_agnostic/id/14'): {
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>'
                ], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>'
                ]
            }, 
            URIRef('https://github.com/arcangelo7/time_agnostic/id/85509'): {
                '2021-06-01T18:46:41+00:00': [], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>'
                ]
            }
        }
        expected_reconstructed_entities = {URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'), URIRef('https://github.com/arcangelo7/time_agnostic/id/85509'), URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'), URIRef('https://github.com/arcangelo7/time_agnostic/id/14')}
        agnostic_query._solve_variables()
        self.assertEqual(_to_dict_of_nt_sorted_lists(agnostic_query.relevant_entities_graphs), expected_relevant_entities_graphs)
        assert (agnostic_query.reconstructed_entities, _to_dict_of_nt_sorted_lists(agnostic_query.relevant_entities_graphs)) == (expected_reconstructed_entities, expected_relevant_entities_graphs)
    
    def test__solve_variables_dead_end(self):
        query = """
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519999> pro:isHeldBy ?o.
                ?o datacite:hasIdentifier ?id.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        agnostic_query.reconstructed_entities = {URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519999')}
        agnostic_query.relevant_entities_graphs = dict()
        expected_reconstructed_entities = {URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519999')}
        expected_relevant_entities_graphs = dict()
        agnostic_query._solve_variables()
        assert (agnostic_query.reconstructed_entities, agnostic_query.relevant_entities_graphs) == (expected_reconstructed_entities, expected_relevant_entities_graphs)

    def test__get_vars_to_explicit_by_time(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        agnostic_query.vars_to_explicit_by_time = dict()
        agnostic_query.relevant_graphs = _to_dict_of_datasets({
            '2021-06-01T18:46:41+00:00': [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
            ], 
            '2021-05-07T09:59:15+00:00': [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
            ], 
            '2021-05-31T18:19:47+00:00': [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
            ]
        })
        agnostic_query._get_vars_to_explicit_by_time()
        expected_output = {
                '2021-06-01T18:46:41+00:00': {
                    (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
                    (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                    (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value')) 
                }, 
                '2021-05-07T09:59:15+00:00': {
                    (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
                    (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                    (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value'))
                }, 
                '2021-05-31T18:19:47+00:00': {
                    (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
                    (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                    (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value'))
                }
            }
        self.assertEqual(agnostic_query.vars_to_explicit_by_time, expected_output) 

    def test__there_are_variables_true(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        agnostic_query.vars_to_explicit_by_time = {
            '2021-06-01T18:46:41+00:00': {
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
                (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value')) 
            }, 
            '2021-05-07T09:59:15+00:00': {
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
                (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value')) 
            }, 
            '2021-05-31T18:19:47+00:00': {
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
                (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value')) 
            }
        }
        output = agnostic_query._there_are_variables()
        self.assertEqual(output, True)

    def test__there_are_variables_false(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        output = agnostic_query._there_are_variables()
        self.assertEqual(output, False)
    
    def test__there_is_transitive_closure_false(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
                ?a pro:isHeldBy <https://github.com/arcangelo7/time_agnostic/ra/4>.
            }
        """    
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        variable = Variable('a')
        triple =  (Variable('a'), URIRef('http://purl.org/spar/pro/isHeldBy'), URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'))
        other_triples = {t for t in agnostic_query.triples if t != triple}
        output = agnostic_query._there_is_transitive_closure(variable, other_triples)
        self.assertEqual(output, False)

    def test__there_is_transitive_closure_true(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }
        """    
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        variable = Variable('id')
        triple =  (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value'))
        other_triples = {t for t in agnostic_query.triples if t != triple}
        output = agnostic_query._there_is_transitive_closure(variable, other_triples)
        self.assertEqual(output, True)

    def test__is_isolated_true_s(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
                ?a pro:isHeldBy <https://github.com/arcangelo7/time_agnostic/ra/4>.
            }
        """        
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        triple =  (Variable('a'), URIRef('http://purl.org/spar/pro/isHeldBy'), URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'))
        output = agnostic_query._is_isolated(triple)
        self.assertEqual(output, True)

    def test__is_isolated_true_s_o(self):
        query = """
            prefix cito: <http://purl.org/spar/cito/>
            prefix datacite: <http://purl.org/spar/datacite/>
            select distinct ?elt_1
            where {
                ?elt_1 datacite:hasIdentifier ?id_1;
                        datacite:hasIdentifier ?id_2.
                FILTER (?id_1 != ?id_2)
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        triple =  (Variable('elt_1'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id_1'))
        output = agnostic_query._is_isolated(triple)
        self.assertEqual(output, True)

    def test__is_isolated_false(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }
        """        
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        triple = (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value'))
        output = agnostic_query._is_isolated(triple)
        self.assertEqual(output, False)
    
    def test___is_a_dead_end(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }
        """        
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        triple_1 = (URIRef("https://github.com/arcangelo7/time_agnostic/ar/15519"), URIRef("http://purl.org/spar/pro/isHeldBy"), Variable("o"))
        is_a_dead_end_1 = agnostic_query._is_a_dead_end(URIRef("https://github.com/arcangelo7/time_agnostic/ar/15519"), triple_1)
        triple_2 = (Variable("id"), URIRef("http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"), Variable("value"))
        is_a_dead_end_2 = agnostic_query._is_a_dead_end(Variable("value"), triple_2)
        is_a_dead_end = (is_a_dead_end_1, is_a_dead_end_2)
        self.assertEqual(is_a_dead_end, (False, True))

    def test__explicit_solvable_variables(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        agnostic_query.vars_to_explicit_by_time = {
            '2021-06-01T18:46:41+00:00': {
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
                (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value')) 
            }, 
            '2021-05-07T09:59:15+00:00': {
                    (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
                    (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                    (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value'))
            }, 
            '2021-05-31T18:19:47+00:00': {
                    (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
                    (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                    (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value'))
            }
        }
        output = agnostic_query._explicit_solvable_variables()
        expected_output = {
            '2021-05-07T09:59:15+00:00': {
                Variable('o'): {
                    (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'),URIRef('http://purl.org/spar/pro/isHeldBy'),URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'))
                }
            },
            '2021-05-31T18:19:47+00:00': {
                Variable('o'): {
                    (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'),URIRef('http://purl.org/spar/pro/isHeldBy'),URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'))
                }
            },
            '2021-06-01T18:46:41+00:00': {
                Variable('o'): {
                    (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'),URIRef('http://purl.org/spar/pro/isHeldBy'),URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'))
                }
            }
        }
        self.assertEqual(output, expected_output)

    def test__update_vars_to_explicit(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        agnostic_query.vars_to_explicit_by_time = {
            '2021-06-01T18:46:41+00:00': {
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
                (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value')) 
            }, 
            '2021-05-07T09:59:15+00:00': {
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
                (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value')) 
            }, 
            '2021-05-31T18:19:47+00:00': {
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), Variable('o')), 
                (Variable('o'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value')) 
            }
        }
        solved_variables = {
            '2021-05-07T09:59:15+00:00': {
                Variable('o'): {
                    (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'),URIRef('http://purl.org/spar/pro/isHeldBy'),URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'))
                }
            },
            '2021-05-31T18:19:47+00:00': {
                Variable('o'): {
                    (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'),URIRef('http://purl.org/spar/pro/isHeldBy'),URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'))
                }
            },
            '2021-06-01T18:46:41+00:00': {
                Variable('o'): {
                    (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'),URIRef('http://purl.org/spar/pro/isHeldBy'),URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'))
                }
            }
        }
        agnostic_query._update_vars_to_explicit(solved_variables)
        expected_output = {
            '2021-05-31T18:19:47+00:00': {
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519')), 
                (URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value'))
            }, 
            '2021-05-07T09:59:15+00:00': {
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519')), 
                (URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value')) 
            }, 
            '2021-06-01T18:46:41+00:00': {
                (URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), URIRef('http://purl.org/spar/pro/isHeldBy'), URIRef('https://github.com/arcangelo7/time_agnostic/ra/4')), 
                (URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'), URIRef('http://purl.org/spar/datacite/hasIdentifier'), Variable('id')),
                (Variable('id'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('value'))
            }
        } 

        self.assertEqual(agnostic_query.vars_to_explicit_by_time, expected_output)

    def test__get_present_entities_inverse_property(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                ?o ^pro:isHeldBy <https://github.com/arcangelo7/time_agnostic/ar/15519>.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        triple = agnostic_query._process_query()[0]
        present_entities = agnostic_query._get_present_entities(triple)
        self.assertEqual(present_entities, {URIRef("https://github.com/arcangelo7/time_agnostic/ar/15519")})

    def test__get_query_to_update_queries(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                ?a literal:hasLiteralValue ?b.
            }
        """
        triple = (Variable('a'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('b'))
        query_to_identify = VersionQuery(query, config_path=CONFIG_PATH)._get_query_to_update_queries(triple).replace(" ", "").replace("\n", "")
        expected_query_to_identify = """
            SELECT ?updateQuery
            WHERE {
                ?snapshot <https://w3id.org/oc/ontology/hasUpdateQuery> ?updateQuery.
                FILTER CONTAINS (?updateQuery, 'http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue').
            }
        """.replace(" ", "").replace("\n", "")
        self.assertEqual(query_to_identify, expected_query_to_identify)

    def test__get_query_to_update_queries_blazegraph(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                ?a literal:hasLiteralValue ?b.
            }
        """
        triple = (Variable('a'), URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), Variable('b'))
        query_to_identify = VersionQuery(query, config_path='tests/config_blazegraph.json')._get_query_to_update_queries(triple).replace(" ", "").replace("\n", "")
        expected_query_to_identify_bds = """
            PREFIX bds: <http://www.bigdata.com/rdf/search#>
            SELECT ?updateQuery
            WHERE {
                ?snapshot <https://w3id.org/oc/ontology/hasUpdateQuery> ?updateQuery.
                ?updateQuery bds:search "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue";
                    bds:matchAllTerms 'true'.
            }
        """.replace(" ", "").replace("\n", "")
        self.assertEqual(query_to_identify, expected_query_to_identify_bds)

    def test__find_entities_in_update_queries(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?a
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ra/4> datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?literalValue.
                ?a pro:isHeldBy <https://github.com/arcangelo7/time_agnostic/ra/15519>.
            }
        """        
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        agnostic_query.relevant_entities_graphs = dict()
        agnostic_query.reconstructed_entities = set()
        triple = (Variable('a'), URIRef('http://purl.org/spar/pro/isHeldBy'), URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'))
        expected_relevant_entities_graphs = {
            URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-31T18:19:47+00:00': [
'<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                    ]
                }
            }
        expected_reconstructed_entities = {URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519')}
        agnostic_query._find_entities_in_update_queries(triple, set())
        self.assertEqual(agnostic_query.reconstructed_entities, expected_reconstructed_entities)
        self.assertEqual(_to_dict_of_nt_sorted_lists(agnostic_query.relevant_entities_graphs), expected_relevant_entities_graphs)
    
    def test_run_agnostic_query_easy(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                    rdf:type pro:RoleInTime.
            }
        """
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            {
                '2021-05-07T09:59:15+00:00': [],
                '2021-05-31T18:19:47+00:00': [
                    {'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/15519'}}],
                '2021-06-01T18:46:41+00:00': [
                    {'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/4'}}]
            },
            set())
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_optional(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o.
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/4> rdf:type pro:RoleInTime.}
            }
        """
        agnostic_query = VersionQuery(query, other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            {
                '2021-06-01T18:46:41+00:00': [
                    {'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/4'}}],
                '2021-05-31T18:19:47+00:00': [
                    {'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/15519'}}],
                '2021-05-07T09:59:15+00:00': [
                    {'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/15519'}}]
            },
            set())
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_more_variables_and_more_optionals(self):
        query = """
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o.
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                OPTIONAL {?id literal:hasLiteralValue ?value.}
            }
        """
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-31T18:19:47+00:00': [
                {'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/15519'},
                 'id': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/id/85509'},
                 'value': {'type': 'literal', 'value': 'http://orcid.org/0000-0002-3259-2309'}}],
            '2021-05-07T09:59:15+00:00': [
                {'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/15519'},
                 'id': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/id/85509'},
                 'value': {'type': 'literal', 'value': 'http://orcid.org/0000-0002-3259-2309'}}],
            '2021-06-01T18:46:41+00:00': [
                {'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/4'},
                 'id': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/id/14'},
                 'value': {'type': 'literal', 'value': 'http://orcid.org/0000-0002-3259-2309'}}]
        }, set())
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_obj_var(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?a ?id
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ra/4> datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?literalValue.
              OPTIONAL {?a pro:isHeldBy <https://github.com/arcangelo7/time_agnostic/ra/15519>.}
            }
        """
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_VIRTUOSO)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15+00:00': [
                {'a': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ar/15519'},
                 'id': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/id/14'}}],
            '2021-05-31T18:19:47+00:00': [
                {'a': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ar/15519'},
                 'id': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/id/14'}}],
            '2021-06-01T18:46:41+00:00': [
                {'id': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/id/14'}}]
        }, set())
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_p_obj_var(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?s ?p
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ra/4> datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?literalValue.
              OPTIONAL {?s ?p <https://github.com/arcangelo7/time_agnostic/ra/15519>.}
            }
        """
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_VIRTUOSO)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15+00:00': [
                {'s': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ar/15519'},
                 'p': {'type': 'uri', 'value': 'http://purl.org/spar/pro/isHeldBy'}}],
            '2021-05-31T18:19:47+00:00': [
                {'s': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ar/15519'},
                 'p': {'type': 'uri', 'value': 'http://purl.org/spar/pro/isHeldBy'}}],
            '2021-06-01T18:46:41+00:00': [{}]
        }, set())
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_subj_obj_var(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?s ?o
            WHERE {
                ?s pro:isHeldBy ?o.
            }        
        """
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15+00:00': [
                {'s': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ar/15519'},
                 'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/15519'}}],
            '2021-05-31T18:19:47+00:00': [
                {'s': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ar/15519'},
                 'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/15519'}}],
            '2021-06-01T18:46:41+00:00': [
                {'s': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ar/15519'},
                 'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/4'}}]
        }, set())
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_var_multi_cont_values(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX cito: <http://purl.org/spar/cito/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            SELECT DISTINCT ?br ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/br/2> cito:cites ?br.
                ?br datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }   
        """
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        br31830 = {'br': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/br/31830'},
                    'id': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/id/4'},
                    'value': {'type': 'literal', 'value': '10.1080/15216540701258751'}}
        br33757 = {'br': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/br/33757'},
                    'id': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/id/27139'},
                    'value': {'type': 'literal', 'value': '10.1007/s11192-006-0133-x'}}
        expected_output = ({
            '2021-05-07T09:59:15+00:00': [],
            '2021-05-30T16:42:28+00:00': [],
            '2021-05-30T18:15:04+00:00': [],
            '2021-05-30T19:41:57+00:00': [br31830],
            '2021-05-30T20:28:47+00:00': [br31830, br33757],
            '2021-05-30T21:29:54+00:00': [br31830, br33757],
            '2021-05-30T23:37:34+00:00': [br31830, br33757],
            '2021-05-31T20:31:01+00:00': [br31830, br33757],
            '2021-05-31T21:55:56+00:00': [br31830, br33757],
            '2021-06-01T01:02:01+00:00': [br31830, br33757],
            '2021-06-30T19:26:15+00:00': [br31830, br33757]
        }, set())
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_easy_on_time(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                    rdf:type pro:RoleInTime.
            }
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-31T18:19:47+00:00", "2021-05-31T18:19:47+00:00"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            {'2021-05-31T18:19:47+00:00': [
                {'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/15519'}}]},
            {'2021-06-01T18:46:41+00:00', '2021-05-07T09:59:15+00:00'})
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_easy_on_time_no_results(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                    rdf:type pro:RoleInTime.
            }
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-06T00:00:00+00:00", "2021-05-06T00:00:00+00:00"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            dict(), 
            {'2021-06-01T18:46:41+00:00', '2021-05-31T18:19:47+00:00', '2021-05-07T09:59:15+00:00'})
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_optional_on_time(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o.
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/4> rdf:type pro:RoleInTime.}
            }
        """
        agnostic_query = VersionQuery(query, on_time=("2021-06-01T18:46:41+00:00", "2021-06-01T18:46:41+00:00"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            {'2021-06-01T18:46:41+00:00': [
                {'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/4'}}]},
            {'2021-05-31T18:19:47+00:00', '2021-05-07T09:59:15+00:00'})
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_more_variables_and_more_optionals_on_time(self):
        query = """
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o.
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                OPTIONAL {?id literal:hasLiteralValue ?value.}
            }
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-07T09:59:15+00:00", "2021-05-07T09:59:15+00:00"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15+00:00': [
                {'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/15519'},
                 'id': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/id/85509'},
                 'value': {'type': 'literal', 'value': 'http://orcid.org/0000-0002-3259-2309'}}]},
            {'2021-06-01T18:46:41+00:00', '2021-05-31T18:19:47+00:00'})
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_obj_var_on_time(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?a ?id
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ra/4> datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?literalValue.
              OPTIONAL {?a pro:isHeldBy <https://github.com/arcangelo7/time_agnostic/ra/15519>.}
            }
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-07T09:59:15+00:00", "2021-05-07T09:59:15+00:00"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15+00:00': [
                {'a': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ar/15519'},
                 'id': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/id/14'}}]},
            {'2021-06-01T18:46:41+00:00', '2021-05-31T18:19:47+00:00'})
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_p_obj_var_on_time(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?s ?p
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ra/4> datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?literalValue.
              OPTIONAL {?s ?p <https://github.com/arcangelo7/time_agnostic/ra/15519>.}
            }
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-07T09:59:15+00:00", "2021-05-07T09:59:15+00:00"), other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15+00:00': [
                {'s': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ar/15519'},
                 'p': {'type': 'uri', 'value': 'http://purl.org/spar/pro/isHeldBy'}}]
        }, set())
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_subj_obj_var_on_time(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?s ?o
            WHERE {
                ?s pro:isHeldBy ?o.
            }        
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-31T18:19:47+00:00", "2021-05-31T18:19:47+00:00"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-31T18:19:47+00:00': [
                {'s': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ar/15519'},
                 'o': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/ra/15519'}}]},
            {'2021-06-01T18:46:41+00:00', '2021-05-07T09:59:15+00:00'})
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    def test_run_agnostic_query_var_multi_cont_values_on_time(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX cito: <http://purl.org/spar/cito/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            SELECT DISTINCT ?br ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/br/2> cito:cites ?br.
                ?br datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }   
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-30T19:41:57+00:00", "2021-05-30T19:41:57+00:00"), other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-30T19:41:57+00:00': [
                {'br': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/br/31830'},
                 'id': {'type': 'uri', 'value': 'https://github.com/arcangelo7/time_agnostic/id/4'},
                 'value': {'type': 'literal', 'value': '10.1080/15216540701258751'}}]
        }, set())
        result, other = output
        expected_result, expected_other = expected_output
        self.assertEqual(other, expected_other)
        self.assertEqual(set(result.keys()), set(expected_result.keys()))
        for ts in expected_result:
            self.assertEqual(_sort_bindings(result[ts]), _sort_bindings(expected_result[ts]))

    # def test_run_agnostic_query_updating_relevant_times(self):
    #     query = """
    #         PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
    #         PREFIX cito: <http://purl.org/spar/cito/>
    #         PREFIX datacite: <http://purl.org/spar/datacite/> 
    #         SELECT DISTINCT ?br ?id ?value
    #         WHERE {
    #             <https://github.com/arcangelo7/time_agnostic/br/69211> cito:cites ?br.
    #             ?br datacite:hasIdentifier ?id.
    #             OPTIONAL {?id literal:hasLiteralValue ?value.}
    #         }   
    #     """
    #     agnostic_query = VersionQuery(query, other_snapshots=True, config_path=CONFIG_PATH)
    #     output = agnostic_query.run_agnostic_query()
    #     expected_output = (
    #         {'2021-09-09T14:34:43': set(), 
    #         '2021-09-13T16:42:27': {
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'identifier', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'identifier', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None)}, 
    #         '2021-09-13T16:43:22': {
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'identifier', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'identifier', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None)},
    #         '2021-09-13T16:45:30': {
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'identifier', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None)}, 
    #         '2021-09-13T16:47:44': {
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'identifier', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None)}, 
    #         '2021-09-13T16:48:32': {
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'identifier', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None)}, 
    #         '2021-09-13T16:50:06': {
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'identifier', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None)}, 
    #         '2021-09-13T16:51:57': {
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None)},
    #         '2021-09-13T17:08:13': {
    #             ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None)},
    #         '2021-09-13T17:09:28': {
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', '10.3109/10673229.2010.493742'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None)},
    #         '2021-09-13T17:10:27': {
    #             ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', '10.1111/j.1365-2702.2009.02927.x'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', '10.3109/10673229.2010.493742'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None)},
    #         '2021-09-13T17:11:16': {
    #             ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', '10.1111/j.1365-2702.2009.02927.x'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', '10.3109/10673229.2010.493742'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', '10.3928/00220124-20100126-03')}, 
    #         '2021-09-13T17:11:59': {
    #             ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', '10.1111/j.1365-2702.2009.02927.x'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', '10.1111/j.1365-2702.2010.03679.x'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', '10.3109/10673229.2010.493742'), 
    #             ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', '10.3928/00220124-20100126-03')}}, set())
    #     self.assertEqual(output, expected_output)


    def test_run_agnostic_query_single_isolated_triple_on_time(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT ?o
            WHERE {
                ?o a pro:RoleInTime .
            }
        """
        vq = VersionQuery(
            query,
            on_time=("2021-05-31T18:19:47+00:00", "2021-05-31T18:19:47+00:00"),
            other_snapshots=False,
            config_dict=CONFIG,
        )
        result, other = vq.run_agnostic_query()
        self.assertEqual(other, set())
        self.assertIn("2021-05-31T18:19:47+00:00", result)
        bindings = result["2021-05-31T18:19:47+00:00"]
        values = {b["o"]["value"] for b in bindings}
        self.assertIn("https://github.com/arcangelo7/time_agnostic/ar/15519", values)

    def test_run_agnostic_query_vm_batch_no_entities(self):
        query = """
            SELECT ?o
            WHERE {
                ?o a <http://example.com/NonExistentType> .
            }
        """
        vq = VersionQuery(
            query,
            on_time=("2021-05-31T18:19:47+00:00", "2021-05-31T18:19:47+00:00"),
            other_snapshots=False,
            config_dict=CONFIG,
        )
        result, other = vq.run_agnostic_query()
        self.assertEqual(result, {})
        self.assertEqual(other, set())

    def test_run_agnostic_query_vm_batch_non_quadstore(self):
        config_no_quad = {**CONFIG, "dataset": {**CONFIG["dataset"], "is_quadstore": False}}
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT ?o
            WHERE {
                ?o a pro:RoleInTime .
            }
        """
        vq = VersionQuery(
            query,
            on_time=("2021-05-31T18:19:47+00:00", "2021-05-31T18:19:47+00:00"),
            other_snapshots=False,
            config_dict=config_no_quad,
        )
        result, other = vq.run_agnostic_query()
        self.assertIn("2021-05-31T18:19:47+00:00", result)
        values = {b["o"]["value"] for b in result["2021-05-31T18:19:47+00:00"]}
        self.assertIn("https://github.com/arcangelo7/time_agnostic/ar/15519", values)

    def test_run_agnostic_query_cross_version_no_entities_fill_timestamps(self):
        query = """
            SELECT ?o
            WHERE {
                ?o a <http://example.com/NonExistentType> .
            }
        """
        vq = VersionQuery(query, config_dict=CONFIG)
        result, other = vq.run_agnostic_query(include_all_timestamps=True)
        self.assertEqual(result, {})
        self.assertEqual(other, set())

    def test_run_agnostic_query_cross_version_with_fill_timestamps(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT ?o
            WHERE {
                ?o a pro:RoleInTime .
            }
        """
        vq = VersionQuery(query, config_dict=CONFIG)
        result, other = vq.run_agnostic_query(include_all_timestamps=True)
        self.assertIsInstance(result, dict)
        self.assertGreater(len(result), 0)

    def test_run_agnostic_query_cross_version_variable_predicate(self):
        query = """
            SELECT ?s ?p
            WHERE {
                ?s ?p <https://github.com/arcangelo7/time_agnostic/ra/15519> .
            }
        """
        vq = VersionQuery(query, config_dict=CONFIG)
        result, other = vq.run_agnostic_query()
        self.assertIsInstance(result, dict)
        found_match = False
        for ts, bindings in result.items():
            for b in bindings:
                if b.get("s", {}).get("value") == "https://github.com/arcangelo7/time_agnostic/ar/15519":
                    self.assertEqual(b["p"]["value"], "http://purl.org/spar/pro/isHeldBy")
                    found_match = True
        self.assertTrue(found_match)

    def test_run_agnostic_query_cross_version_non_isolated(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT ?ar ?agent
            WHERE {
                ?ar a pro:RoleInTime .
                ?ar pro:isHeldBy ?agent .
            }
        """
        vq = VersionQuery(query, config_dict=CONFIG)
        result, other = vq.run_agnostic_query()
        self.assertIsInstance(result, dict)
        self.assertGreater(len(result), 0)
        for ts, bindings in result.items():
            for b in bindings:
                self.assertIn("ar", b)
                self.assertIn("agent", b)


    @patch('time_agnostic_library.agnostic_query._PARALLEL_THRESHOLD', 1)
    def test_run_agnostic_query_parallel_execution(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT ?ar
            WHERE {
                ?ar a pro:RoleInTime.
                ?ar pro:isHeldBy ?agent.
            }
        """
        vq = VersionQuery(query=query, on_time=("2021-05-31T18:19:47+00:00", "2021-05-31T18:19:47+00:00"), config_path=CONFIG_PATH)
        result, _ = vq.run_agnostic_query()
        self.assertIn("2021-05-31T18:19:47+00:00", result)
        values = {b["ar"]["value"] for b in result["2021-05-31T18:19:47+00:00"]}
        self.assertIn("https://github.com/arcangelo7/time_agnostic/ar/15519", values)

    @patch('time_agnostic_library.agnostic_query._MP_CONTEXT', None)
    @patch('time_agnostic_library.agnostic_query._PARALLEL_THRESHOLD', 1)
    def test_run_agnostic_query_parallel_thread_fallback(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT ?ar
            WHERE {
                ?ar a pro:RoleInTime.
                ?ar pro:isHeldBy ?agent.
            }
        """
        vq = VersionQuery(query=query, on_time=("2021-05-31T18:19:47+00:00", "2021-05-31T18:19:47+00:00"), config_path=CONFIG_PATH)
        result, _ = vq.run_agnostic_query()
        self.assertIn("2021-05-31T18:19:47+00:00", result)
        values = {b["ar"]["value"] for b in result["2021-05-31T18:19:47+00:00"]}
        self.assertIn("https://github.com/arcangelo7/time_agnostic/ar/15519", values)


if __name__ == '__main__': # pragma: no cover
    unittest.main()