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
import os
import unittest
from pprint import pprint

import rdflib
from SPARQLWrapper import JSON, POST, SPARQLWrapper

from time_agnostic_library.agnostic_query import VersionQuery
from time_agnostic_library.support import (_to_conjunctive_graph,
                                           _to_dict_of_conjunctive_graphs,
                                           _to_dict_of_nt_sorted_lists,
                                           empty_the_cache)

CONFIG_PATH = os.path.join('tests', 'config.json')
CONFIG_BLAZEGRAPH = os.path.join('tests', 'config_blazegraph.json')

class Test_VersionQuery(unittest.TestCase):  
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
                rdflib.term.Variable('o')
            },
            'triples': [
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/2'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o'))
            ]
        }
        input2 = "triples"
        output = list()
        VersionQuery(query, config_path=CONFIG_PATH)._tree_traverse(input1, input2, output)
        expected_output = [(rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/2'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o'))]
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
                rdflib.term.Variable('o')},
            'triples': [
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/2'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),rdflib.term.URIRef('http://purl.org/spar/pro/RoleInTime')),
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/2'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o'))
            ]
        }
        input2 = "triples"
        output = list()
        VersionQuery(query, config_path=CONFIG_PATH)._tree_traverse(input1, input2, output)
        expected_output = [
            (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/2'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), rdflib.term.URIRef('http://purl.org/spar/pro/RoleInTime')), 
            (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/2'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o'))
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
                rdflib.term.Variable('id'),
                rdflib.term.Variable('o'),
                rdflib.term.Variable('value')
            },
            'expr': {
                '_vars': set()
            },
            'p1': {
                '_vars': {
                    rdflib.term.Variable('o'), 
                    rdflib.term.Variable('id')
                },
                'lazy': True,
                'p1': {
                    '_vars': {rdflib.term.Variable('o')},
                    'expr': {'_vars': set()},
                    'p1': {'_vars': {rdflib.term.Variable('o')},
                    'triples': [(rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o'))]},
                    'p2': {'_vars': set(),
                    'triples': [(rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), rdflib.term.URIRef('http://purl.org/spar/pro/RoleInTime'))]}
                },
                'p2': {
                    '_vars': {rdflib.term.Variable('o'), rdflib.term.Variable('id')},
                    'triples': [(rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id'))]
                }
            },
            'p2': {
                '_vars': {rdflib.term.Variable('id'), rdflib.term.Variable('value')},
                    'triples': [(rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value'))]
            }
        }
        input2 = "triples"
        output = list()
        VersionQuery(query, config_path=CONFIG_PATH)._tree_traverse(input1, input2, output)
        expected_output = [
            (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
            (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), rdflib.term.URIRef('http://purl.org/spar/pro/RoleInTime')), 
            (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')), 
            (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value'))
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
            (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
            (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), rdflib.term.URIRef('http://purl.org/spar/pro/RoleInTime')), 
            (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')), 
            (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value'))
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
        agnostic_query.relevant_entities_graphs = _to_dict_of_conjunctive_graphs({
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
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
        agnostic_query.relevant_entities_graphs = _to_dict_of_conjunctive_graphs({
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
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
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'): {
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
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'): {
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ], 
                '2021-06-01T18:46:41+00:00': []
            }, 
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/id/14'): {
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
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/id/85509'): {
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
        agnostic_query.relevant_entities_graphs = _to_dict_of_conjunctive_graphs({
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
                '2021-05-31T18:19:47': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-06-01T18:46:41': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-07T09:59:15': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ]
            }
        })
        entity = rdflib.URIRef("https://github.com/arcangelo7/time_agnostic/ra/15519")
        agnostic_query.reconstructed_entities = {rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519')}
        agnostic_query._rebuild_relevant_entity(entity)
        expected_relevant_entities_graphs = {
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
                '2021-05-31T18:19:47': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-06-01T18:46:41': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], '2021-05-07T09:59:15': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ]
            },
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'): {
                '2021-06-01T18:46:41': [], 
                '2021-05-07T09:59:15': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ]
            }
        }
        expected_reconstructed_entities = {rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519')}
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
        agnostic_query.reconstructed_entities = {rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519')}
        agnostic_query.relevant_entities_graphs = _to_dict_of_conjunctive_graphs({
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
                '2021-06-01T18:46:41': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-07T09:59:15': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-31T18:19:47': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ]
            }
        })
        expected_relevant_entities_graphs = {
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
                '2021-06-01T18:46:41': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-07T09:59:15': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-31T18:19:47': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ]
            }, 
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'): {
                '2021-06-01T18:46:41': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ], 
                '2021-05-07T09:59:15': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ]
            }, 
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'): {
                '2021-06-01T18:46:41': [], 
                '2021-05-07T09:59:15': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ]
            }, 
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/id/14'): {
                '2021-06-01T18:46:41': [
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>'
                ], 
                '2021-05-07T09:59:15': [
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>'
                ]
            }, 
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/id/85509'): {
                '2021-06-01T18:46:41': [
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>'
                ], 
                '2021-05-07T09:59:15': [
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>'
                ]
            }
        }
        expected_reconstructed_entities = {rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/id/85509'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/id/14')}
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
        agnostic_query.reconstructed_entities = {rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519999')}
        agnostic_query.relevant_entities_graphs = dict()
        expected_reconstructed_entities = {rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519999')}
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
        agnostic_query.relevant_graphs = _to_dict_of_conjunctive_graphs({
            '2021-06-01T18:46:41': [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
            ], 
            '2021-05-07T09:59:15': [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
            ], 
            '2021-05-31T18:19:47': [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
            ]
        })
        agnostic_query._get_vars_to_explicit_by_time()
        expected_output = {
                '2021-06-01T18:46:41': {
                    (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
                    (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                    (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value')) 
                }, 
                '2021-05-07T09:59:15': {
                    (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
                    (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                    (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value'))
                }, 
                '2021-05-31T18:19:47': {
                    (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
                    (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                    (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value'))
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
            '2021-06-01T18:46:41': {
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
                (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value')) 
            }, 
            '2021-05-07T09:59:15': {
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
                (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value')) 
            }, 
            '2021-05-31T18:19:47': {
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
                (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value')) 
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
        variable = rdflib.term.Variable('a')
        triple =  (rdflib.term.Variable('a'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'))
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
        variable = rdflib.term.Variable('id')
        triple =  (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value'))
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
        triple =  (rdflib.term.Variable('a'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'))
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
        triple =  (rdflib.term.Variable('elt_1'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id_1'))
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
        triple = (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value'))
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
        triple_1 = (rdflib.term.URIRef("https://github.com/arcangelo7/time_agnostic/ar/15519"), rdflib.term.URIRef("http://purl.org/spar/pro/isHeldBy"), rdflib.term.Variable("o"))
        is_a_dead_end_1 = agnostic_query._is_a_dead_end(rdflib.term.URIRef("https://github.com/arcangelo7/time_agnostic/ar/15519"), triple_1)
        triple_2 = (rdflib.term.Variable("id"), rdflib.term.URIRef("http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"), rdflib.term.Variable("value"))
        is_a_dead_end_2 = agnostic_query._is_a_dead_end(rdflib.term.Variable("value"), triple_2)
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
            '2021-06-01T18:46:41': {
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
                (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value')) 
            }, 
            '2021-05-07T09:59:15': {
                    (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
                    (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                    (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value'))
            }, 
            '2021-05-31T18:19:47': {
                    (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
                    (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                    (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value'))
            }
        }
        output = agnostic_query._explicit_solvable_variables()
        expected_output = {
            '2021-05-07T09:59:15': {
                rdflib.term.Variable('o'): {
                    (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'),rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'),rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'))
                }
            },
            '2021-05-31T18:19:47': {
                rdflib.term.Variable('o'): {
                    (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'),rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'),rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'))
                }
            },
            '2021-06-01T18:46:41': {
                rdflib.term.Variable('o'): {
                    (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'),rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'),rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'))
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
            '2021-06-01T18:46:41': {
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
                (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value')) 
            }, 
            '2021-05-07T09:59:15': {
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
                (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value')) 
            }, 
            '2021-05-31T18:19:47': {
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.Variable('o')), 
                (rdflib.term.Variable('o'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value')) 
            }
        }
        solved_variables = {
            '2021-05-07T09:59:15': {
                rdflib.term.Variable('o'): {
                    (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'),rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'),rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'))
                }
            },
            '2021-05-31T18:19:47': {
                rdflib.term.Variable('o'): {
                    (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'),rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'),rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'))
                }
            },
            '2021-06-01T18:46:41': {
                rdflib.term.Variable('o'): {
                    (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'),rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'),rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'))
                }
            }
        }
        agnostic_query._update_vars_to_explicit(solved_variables)
        expected_output = {
            '2021-05-31T18:19:47': {
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519')), 
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value'))
            }, 
            '2021-05-07T09:59:15': {
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519')), 
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value')) 
            }, 
            '2021-06-01T18:46:41': {
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/4')), 
                (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'), rdflib.term.URIRef('http://purl.org/spar/datacite/hasIdentifier'), rdflib.term.Variable('id')),
                (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value'))
            }
        } 

        self.assertEqual(agnostic_query.vars_to_explicit_by_time, expected_output)

    def test__get_query_to_identify_inverse_property(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                ?o ^pro:isHeldBy <https://github.com/arcangelo7/time_agnostic/ar/15519>.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        triple = agnostic_query._process_query()[0]
        query_to_identify = agnostic_query._get_query_to_identify(triple).replace(" ", "").replace("\n", "")
        expected_query_to_identify = """
            CONSTRUCT { 
                <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> ?o 
            } WHERE { 
                ?o ^<http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ar/15519>
            }
        """.replace(" ", "").replace("\n", "")
        self.assertEqual(query_to_identify, expected_query_to_identify)

    def test__get_query_to_update_queries(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                ?a literal:hasLiteralValue ?b.
            }
        """
        triple = (rdflib.term.Variable('a'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('b'))
        query_to_identify = VersionQuery(query, config_path=CONFIG_PATH)._get_query_to_update_queries(triple).replace(" ", "").replace("\n", "")
        expected_query_to_identify = """
            SELECT DISTINCT ?updateQuery 
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
        triple = (rdflib.term.Variable('a'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('b'))
        query_to_identify = VersionQuery(query, config_path=CONFIG_BLAZEGRAPH)._get_query_to_update_queries(triple).replace(" ", "").replace("\n", "")
        expected_query_to_identify_bds = """
            PREFIX bds: <http://www.bigdata.com/rdf/search#>
            SELECT DISTINCT ?updateQuery
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
        triple = (rdflib.term.Variable('a'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'))
        expected_relevant_entities_graphs = {
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'): {
                '2021-05-07T09:59:15': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-06-01T18:46:41': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-31T18:19:47': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                    ]
                }
            }
        expected_reconstructed_entities = {rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519')}
        agnostic_query._find_entities_in_update_queries(triple, set())
        assert (agnostic_query.reconstructed_entities, _to_dict_of_nt_sorted_lists(agnostic_query.relevant_entities_graphs)) == (expected_reconstructed_entities, expected_relevant_entities_graphs)
    
    def test__cache_entity_graph(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                    rdf:type pro:RoleInTime.
            }
        """
        agnostic_query = VersionQuery(query, on_time=('2021-05-07T09:59:15', '2021-05-07T09:59:15'), config_path=CONFIG_PATH)
        if agnostic_query.cache_endpoint:
            recostructed_grah = _to_conjunctive_graph([
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
            ])
            with open(CONFIG_PATH, encoding="utf8") as json_file:
                cache_url = json.load(json_file)["cache_triplestore_url"]["update_endpoint"]
            sparql = SPARQLWrapper(cache_url)
            delete_query = """
                DELETE DATA { GRAPH <https://github.com/opencitations/time-agnostic-library/2021-05-07T09:59:15> {
                    <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>. 
                    <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>. 
                    <https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>.
                    }
                }
            """
            sparql.setQuery(delete_query)
            sparql.setMethod(POST)
            sparql.query()
            prov_metadata = {
                rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'): {
                    'https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1': {
                        'http://www.w3.org/ns/prov#generatedAtTime': '2021-05-07T09:59:15', 
                        'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                        'http://www.w3.org/ns/prov#hadPrimarySource': None, 'http://purl.org/dc/terms/description': "The entity 'https://github.com/arcangelo7/time_agnostic/ra/15519' has been created."
                    }, 
                    'https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/2': {
                        'http://www.w3.org/ns/prov#generatedAtTime': '2021-06-01T18:46:41', 
                        'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                        'http://www.w3.org/ns/prov#hadPrimarySource': None, 'http://purl.org/dc/terms/description': "The entity 'https://github.com/arcangelo7/time_agnostic/ra/15519' has been deleted."
                    }
                }
            }
            agnostic_query._cache_entity_graph("https://github.com/arcangelo7/time_agnostic/ar/15519", recostructed_grah, "2021-05-07T09:59:15", prov_metadata)
            sparql.setQuery('; '.join(agnostic_query.cache_insert_queries))
            sparql.query()
            query = """
                CONSTRUCT {?s ?p ?o}
                WHERE {GRAPH <https://github.com/opencitations/time-agnostic-library/2021-05-07T09:59:15> {<https://github.com/arcangelo7/time_agnostic/ar/15519> ?p ?o. ?s ?p ?o.}}
            """
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.queryAndConvert()
            assert (bool(results["results"]["bindings"]) == True)

    def test__get_relevant_timestamps_from_cache(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                    rdf:type pro:RoleInTime.
            }
        """
        agnostic_query = VersionQuery(query, config_path=CONFIG_PATH)
        agnostic_query.run_agnostic_query()
        if agnostic_query.cache_endpoint:
            output = agnostic_query._get_relevant_timestamps_from_cache("https://github.com/arcangelo7/time_agnostic/ar/15519")
            self.assertEqual(set(output.keys()), {'2021-05-07T09:59:15', '2021-06-01T18:46:41', '2021-05-31T18:19:47'})

    def test__no_relevant_timestamps_from_cache(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                    rdf:type pro:RoleInTime.
            }
        """
        empty_the_cache(config_path=CONFIG_PATH)
        agnostic_query = VersionQuery(query, on_time=('2022-12-01', None), config_path=CONFIG_PATH)
        agnostic_query.run_agnostic_query()
        output = _to_dict_of_nt_sorted_lists(agnostic_query._get_relevant_timestamps_from_cache("https://github.com/arcangelo7/time_agnostic/ar/15519"))
        self.assertEqual(output, {'2022-12-01T00:00:00': []})

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
            {'2021-05-07T09:59:15': set(), '2021-05-31T18:19:47': {('https://github.com/arcangelo7/time_agnostic/ra/15519',)}, '2021-06-01T18:46:41': {('https://github.com/arcangelo7/time_agnostic/ra/4',)}},
            set())
        self.assertEqual(output, expected_output)

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
            {'2021-06-01T18:46:41': {('https://github.com/arcangelo7/time_agnostic/ra/4',)}, '2021-05-31T18:19:47': {('https://github.com/arcangelo7/time_agnostic/ra/15519',)}, '2021-05-07T09:59:15': {('https://github.com/arcangelo7/time_agnostic/ra/15519',)}},
            set())
        self.assertEqual(output, expected_output)

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
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ra/15519', 
                'https://github.com/arcangelo7/time_agnostic/id/85509', 
                'http://orcid.org/0000-0002-3259-2309')
            }, 
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ra/15519', 
                'https://github.com/arcangelo7/time_agnostic/id/85509', 
                'http://orcid.org/0000-0002-3259-2309')
            }, 
            '2021-06-01T18:46:41': {
                ('https://github.com/arcangelo7/time_agnostic/ra/4', 
                'https://github.com/arcangelo7/time_agnostic/id/14', 
                'http://orcid.org/0000-0002-3259-2309')
            }
        }, set())
        self.assertEqual(output, expected_output)

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
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_BLAZEGRAPH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519', 'https://github.com/arcangelo7/time_agnostic/id/14')
            },
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519', 'https://github.com/arcangelo7/time_agnostic/id/14')
            },
            '2021-06-01T18:46:41': {
                (None, 'https://github.com/arcangelo7/time_agnostic/id/14')
            }
        }, set())
        self.assertEqual(output, expected_output)

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
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_BLAZEGRAPH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','http://purl.org/spar/pro/isHeldBy')
            },
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','http://purl.org/spar/pro/isHeldBy')
            },
            '2021-06-01T18:46:41': {(None, None)}
        }, set())
        self.assertEqual(output, expected_output)

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
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/15519')
            },
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/15519')
            },
            '2021-06-01T18:46:41': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/4')
            }
        }, set())
        self.assertEqual(output, expected_output)

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
        expected_output = ({
            '2021-05-07T09:59:15': set(),
            '2021-05-30T16:42:28': set(),
            '2021-05-30T18:15:04': set(),
            '2021-05-30T19:41:57': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751')},
            '2021-05-30T20:28:47': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')},
            '2021-05-30T21:29:54': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')},
            '2021-05-30T23:37:34': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')},
            '2021-05-31T20:31:01': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')},
            '2021-05-31T21:55:56': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')},
            '2021-06-01T01:02:01': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')},
            '2021-06-30T19:26:15': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')
            }
        }, set())
        self.assertEqual(output, expected_output)

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
        agnostic_query = VersionQuery(query, on_time=("2021-05-31T18:19:47", "2021-05-31T18:19:47"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            {'2021-05-31T18:19:47': {('https://github.com/arcangelo7/time_agnostic/ra/15519',)}},
            {'2021-06-01T18:46:41+00:00', '2021-05-07T09:59:15+00:00'})
        self.assertEqual(output, expected_output)

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
        agnostic_query = VersionQuery(query, on_time=("2021-05-06", "2021-05-06"), other_snapshots=True, config_path=CONFIG_PATH)
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
        agnostic_query = VersionQuery(query, on_time=("2021-06-01T18:46:41", "2021-06-01T18:46:41"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            {'2021-06-01T18:46:41': {('https://github.com/arcangelo7/time_agnostic/ra/4',)}},
            {'2021-05-31T18:19:47+00:00', '2021-05-07T09:59:15+00:00'})
        self.assertEqual(output, expected_output)

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
        agnostic_query = VersionQuery(query, on_time=("2021-05-07T09:59:15", "2021-05-07T09:59:15"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ra/15519', 
                'https://github.com/arcangelo7/time_agnostic/id/85509', 
                'http://orcid.org/0000-0002-3259-2309')
            }},
            {'2021-06-01T18:46:41+00:00', '2021-05-31T18:19:47+00:00'})
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_obj_var_on_time(self):
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
        agnostic_query = VersionQuery(query, on_time=("2021-05-07T09:59:15", "2021-05-07T09:59:15"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519', 'https://github.com/arcangelo7/time_agnostic/id/14')
            }},
            {'2021-06-01T18:46:41+00:00', '2021-05-31T18:19:47+00:00'})
        self.assertEqual(output, expected_output)

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
        agnostic_query = VersionQuery(query, on_time=("2021-05-07T09:59:15", "2021-05-07T09:59:15"), other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','http://purl.org/spar/pro/isHeldBy')
            }
        }, set())
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_subj_obj_var_on_time(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?s ?o
            WHERE {
                ?s pro:isHeldBy ?o.
            }        
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-31T18:19:47", "2021-05-31T18:19:47"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/15519')
            }},
            {'2021-06-01T18:46:41+00:00', '2021-05-07T09:59:15+00:00'})
        self.assertEqual(output, expected_output)

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
        agnostic_query = VersionQuery(query, on_time=("2021-05-30T19:41:57", "2021-05-30T19:41:57"), other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-30T19:41:57': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751')
            }
        }, set())
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_updating_relevant_times(self):
        query = """
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX cito: <http://purl.org/spar/cito/>
            PREFIX datacite: <http://purl.org/spar/datacite/> 
            SELECT DISTINCT ?br ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/br/69211> cito:cites ?br.
                ?br datacite:hasIdentifier ?id.
                OPTIONAL {?id literal:hasLiteralValue ?value.}
            }   
        """
        agnostic_query = VersionQuery(query, other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            {'2021-09-09T14:34:43': set(), 
            '2021-09-13T16:42:27': {
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'identifier', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'identifier', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None)}, 
            '2021-09-13T16:43:22': {
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'identifier', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'identifier', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None)},
            '2021-09-13T16:45:30': {
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'identifier', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None)}, 
            '2021-09-13T16:47:44': {
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'identifier', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None)}, 
            '2021-09-13T16:48:32': {
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'identifier', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None)}, 
            '2021-09-13T16:50:06': {
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'identifier', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None)}, 
            '2021-09-13T16:51:57': {
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None)},
            '2021-09-13T17:08:13': {
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None)},
            '2021-09-13T17:09:28': {
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', '10.3109/10673229.2010.493742'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None)},
            '2021-09-13T17:10:27': {
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', '10.1111/j.1365-2702.2009.02927.x'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', '10.3109/10673229.2010.493742'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None)},
            '2021-09-13T17:11:16': {
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', '10.1111/j.1365-2702.2009.02927.x'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', '10.3109/10673229.2010.493742'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', '10.3928/00220124-20100126-03')}, 
            '2021-09-13T17:11:59': {
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', '10.1111/j.1365-2702.2009.02927.x'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', '10.1111/j.1365-2702.2010.03679.x'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', '10.3109/10673229.2010.493742'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', '10.3928/00220124-20100126-03')}}, set())
        self.assertEqual(output, expected_output)


if __name__ == '__main__': # pragma: no cover
    unittest.main()