#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2021, Arcangelo Massari <arcangelomas@gmail.com>
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

import unittest, datetime, rdflib

from rdflib.graph import ConjunctiveGraph
from pprint import pprint

from time_agnostic_library.sparql import Sparql
from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.agnostic_entity import AgnosticEntity
from time_agnostic_library.agnostic_query import AgnosticQuery, BlazegraphQuery
from time_agnostic_library.support import FileManager, _to_dict_of_nt_sorted_lists, _to_nt_sorted_list, _to_dict_of_conjunctive_graphs, _to_conjunctive_graph

class Test_AgnosticEntity(unittest.TestCase):
    def test_get_history(self):
        input = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        entity_history = AgnosticEntity(input).get_history()
        output = (_to_dict_of_nt_sorted_lists(entity_history[0]), entity_history[1])
        expected_output = ({
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
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
        }, None)
        self.assertEqual(output, expected_output)

    def test_get_history_with_metadata(self):
        input = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        output = AgnosticEntity(input).get_history(include_prov_metadata=True)
        output_0 = _to_dict_of_nt_sorted_lists(output[0])
        output_1 = output[1]
        expected_output_0 = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
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
        }
        expected_output_1 = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-06-01T18:46:41', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://purl.org/dc/terms/description': "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified.",
                    'http://www.w3.org/ns/prov#hadPrimarySource': None 
                }, 
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-05-07T09:59:15', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://purl.org/dc/terms/description': "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been created.",
                    'http://www.w3.org/ns/prov#hadPrimarySource': None
                }, 
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-05-31T18:19:47', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://purl.org/dc/terms/description': "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified.",
                    'http://www.w3.org/ns/prov#hadPrimarySource': None
                }
            }
        }
        assert (output_0 == expected_output_0) and (output_1 == expected_output_1)
    
    def test_get_history_and_related_entities(self):
        input = "https://github.com/arcangelo7/time_agnostic/id/14"
        output = AgnosticEntity(input, related_entities_history=True).get_history(include_prov_metadata=False)
        output_0 = _to_dict_of_nt_sorted_lists(output[0])
        expected_output_0 = {
            'https://github.com/arcangelo7/time_agnostic/ra/4': {
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
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"']
            }, 
            'https://github.com/arcangelo7/time_agnostic/id/14': {
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
            }
        }
        self.assertEqual(output_0, expected_output_0)

    def test_get_history_and_related_entities_with_metadata(self):
        input = "https://github.com/arcangelo7/time_agnostic/id/14"
        output = AgnosticEntity(input, related_entities_history=True).get_history(include_prov_metadata=True)
        output_0 = _to_dict_of_nt_sorted_lists(output[0])
        output_1 = output[1]
        expected_output_0 = {
            'https://github.com/arcangelo7/time_agnostic/ra/4': {
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
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"']
            }, 
            'https://github.com/arcangelo7/time_agnostic/id/14': {
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
            }
        }
        expected_output_1 = {
            'https://github.com/arcangelo7/time_agnostic/id/14': {
                'https://github.com/arcangelo7/time_agnostic/id/14/prov/se/2': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-06-01T18:46:41', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://purl.org/dc/terms/description': "The entity 'https://github.com/arcangelo7/time_agnostic/id/14' has been merged with 'https://github.com/arcangelo7/time_agnostic/id/85509'.",
                    'http://www.w3.org/ns/prov#hadPrimarySource': None
                }, 
                'https://github.com/arcangelo7/time_agnostic/id/14/prov/se/1': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-05-07T09:59:15', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://purl.org/dc/terms/description': "The entity 'https://github.com/arcangelo7/time_agnostic/id/14' has been created.",
                    'http://www.w3.org/ns/prov#hadPrimarySource': None
                }
            }, 
            'https://github.com/arcangelo7/time_agnostic/ra/4': {
                'https://github.com/arcangelo7/time_agnostic/ra/4/prov/se/1': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-05-07T09:59:15', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://purl.org/dc/terms/description': "The entity 'https://github.com/arcangelo7/time_agnostic/ra/4' has been created.",
                    'http://www.w3.org/ns/prov#hadPrimarySource': None
                }, 
                'https://github.com/arcangelo7/time_agnostic/ra/4/prov/se/2': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-06-01T18:46:41', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://purl.org/dc/terms/description': "The entity 'https://github.com/arcangelo7/time_agnostic/ra/4' has been merged with 'https://github.com/arcangelo7/time_agnostic/ra/15519'.",
                    'http://www.w3.org/ns/prov#hadPrimarySource': None
                }
            }
        }
        pprint(output_1)
        assert (output_0 == expected_output_0) and (output_1 == expected_output_1)

    def test_get_state_at_time_no_hooks(self):
        input_1 = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        input_2 = "2021-05-31T19:19:47+00:00"
        output = AgnosticEntity(input_1).get_state_at_time(input_2, include_prov_metadata=False)
        output = (_to_nt_sorted_list(output[0]), output[1], output[2])
        expected_output = (
            [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
            ], 
            {
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-05-31T18:19:47.000Z', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://www.w3.org/ns/prov#hadPrimarySource': None
                }
            },
            None
        )
        self.assertEqual(output, expected_output)

    def test_get_state_at_time_with_hooks(self):
        input_1 = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        input_2 = "2021-05-31T19:19:47+00:00"
        output = AgnosticEntity(input_1).get_state_at_time(input_2, include_prov_metadata=True)
        output = (_to_nt_sorted_list(output[0]), output[1], output[2])
        expected_output = (
            [
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
            ], 
            {
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-05-31T18:19:47.000Z', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://www.w3.org/ns/prov#hadPrimarySource': None
                }
            }, 
            {
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-06-01T18:46:41.000Z', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://www.w3.org/ns/prov#hadPrimarySource': None
                }, 
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-05-07T09:59:15.000Z', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://www.w3.org/ns/prov#hadPrimarySource': None
                }
            }
        )
        self.assertEqual(output, expected_output)

    def test__get_entity_current_state(self):
        input = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        output = _to_dict_of_nt_sorted_lists(AgnosticEntity(input)._get_entity_current_state()[0])
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
                rdflib.term.Literal('2021-06-01T18:46:41+00:00', datatype=rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#dateTime')): [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-07T09:59:15+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-31T18:19:47+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <https://w3id.org/oc/ontology/hasUpdateQuery> "INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime> .} }"^^<http://www.w3.org/2001/XMLSchema#string>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://www.w3.org/ns/prov#generatedAtTime> "2021-06-01T18:46:41+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <https://w3id.org/oc/ontology/hasUpdateQuery> "DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }"^^<http://www.w3.org/2001/XMLSchema#string>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                rdflib.term.Literal('2021-05-31T18:19:47+00:00', datatype=rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#dateTime')): None, 
                rdflib.term.Literal('2021-05-07T09:59:15+00:00', datatype=rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#dateTime')): None}}
        self.assertEqual(output, expected_output)
    
    def test__old_graphs(self):
        input_1 = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        input_2 = AgnosticEntity(input_1)._get_entity_current_state()
        output = _to_dict_of_nt_sorted_lists(AgnosticEntity(input_1)._get_old_graphs(input_2)[0])
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
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
        }
        self.assertEqual(output, expected_output)

    def test__include_prov_metadata(self):
        triples_generated_at_time = [
            (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2'), rdflib.term.URIRef('http://www.w3.org/ns/prov#generatedAtTime'), rdflib.term.Literal('2021-05-31T18:19:47+00:00', datatype=rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#dateTime'))), 
            (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3'), rdflib.term.URIRef('http://www.w3.org/ns/prov#generatedAtTime'), rdflib.term.Literal('2021-06-01T18:46:41+00:00', datatype=rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#dateTime'))), 
            (rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1'), rdflib.term.URIRef('http://www.w3.org/ns/prov#generatedAtTime'), rdflib.term.Literal('2021-05-07T09:59:15+00:00', datatype=rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#dateTime')))
        ]
        current_state = _to_conjunctive_graph([
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-07T09:59:15+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://www.w3.org/ns/prov#wasAttributedTo> <https://orcid.org/0000-0002-8420-0696>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://www.w3.org/ns/prov#hadPrimarySource> "http://api.crossref.org/journals/0138-9130"', 
            '''<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://purl.org/dc/terms/description> "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been created."''', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-31T18:19:47+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#wasAttributedTo> <https://orcid.org/0000-0002-8420-0696>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <https://w3id.org/oc/ontology/hasUpdateQuery> "INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime> .} }"^^<http://www.w3.org/2001/XMLSchema#string>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#hadPrimarySource> "http://api.crossref.org/journals/0138-9130"', 
            '''<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://purl.org/dc/terms/description> "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified."''', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://www.w3.org/ns/prov#generatedAtTime> "2021-06-01T18:46:41+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://www.w3.org/ns/prov#wasAttributedTo> <https://orcid.org/0000-0002-8420-0696>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <https://w3id.org/oc/ontology/hasUpdateQuery> "DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }"^^<http://www.w3.org/2001/XMLSchema#string>', 
            '''<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://purl.org/dc/terms/description> "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified."''', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
        ])
        agnostic_entity = AgnosticEntity("https://github.com/arcangelo7/time_agnostic/ar/15519")
        output = agnostic_entity._include_prov_metadata(triples_generated_at_time, current_state)
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-06-01T18:46:41', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://purl.org/dc/terms/description': "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified.",
                    'http://www.w3.org/ns/prov#hadPrimarySource': None 
                }, 
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-05-07T09:59:15', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://purl.org/dc/terms/description': "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been created.",
                    'http://www.w3.org/ns/prov#hadPrimarySource': "http://api.crossref.org/journals/0138-9130"
                }, 
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2': {
                    'http://www.w3.org/ns/prov#generatedAtTime': '2021-05-31T18:19:47', 
                    'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'http://purl.org/dc/terms/description': "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified.",
                    'http://www.w3.org/ns/prov#hadPrimarySource': "http://api.crossref.org/journals/0138-9130"
                }
            }
        }
        self.assertEqual(output, expected_output)

    def test__manage_long_update_queries_insert_data_only(self):
        input_1 = ConjunctiveGraph()
        input_2 = """
            DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/br/> { 
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/pro/isDocumentContextFor> <https://github.com/arcangelo7/time_agnostic/ar/2995> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15717> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12885> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12855> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12894> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12833> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12875> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12900> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/dc/terms/title> 
                "Citation histories of scientific publications. The data sources"^^<http://www.w3.org/2001/XMLSchema#string> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12892> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12872> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15713> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12859> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12883> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15665> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12853> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15725> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12870> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12886> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12824> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15723> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15660> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15702> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12823> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12864> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12908> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12917> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15748> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15686> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12916> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15680> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15757> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15678> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12844> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15746> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12862> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12921> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15739> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12888> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15659> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15685> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12832> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15704> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15735> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12831> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12882> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12902> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15756> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12877> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12871> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15692> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15699> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12881> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12889> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12825> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12914> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12912> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12841> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12913> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12838> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15722> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15721> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12865> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15733> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15726> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12849> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15697> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12884> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15698> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12904> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15701> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#partOf> <https://github.com/arcangelo7/time_agnostic/br/15657> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12878> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12876> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15681> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15716> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12856> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15740> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12879> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15658> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15719> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12848> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12822> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15693> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15664> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12899> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12905> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15663> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12897> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12827> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12826> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15661> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15676> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15747> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15755> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15668> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15684> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15696> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15734> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12919> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15683> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12866> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15669> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15728> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12911> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15743> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12898> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15750> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12907> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12893> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15712> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12869> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15674> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#embodiment> <https://github.com/arcangelo7/time_agnostic/re/2015> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12837> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15707> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15662> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15666> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15752> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12920> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15718> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15738> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15753> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12821> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12846> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15689> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12918> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15729> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12903> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12895> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12836> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12851> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15709> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15742> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12861> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12915> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15673> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15679> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15671> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15758> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15751> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15670> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12867> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15691> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12839> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12896> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15675> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12890> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15714> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12842> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> 
                "1985-01-01"^^<http://www.w3.org/2001/XMLSchema#gYear> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15754> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15727> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15688> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15737> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15690> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12834> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15677> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12860> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12828> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15672> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15667> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15695> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15705> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15687> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12857> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12887> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15682> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12847> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12854> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15706> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalArticle> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15715> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/13890> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15736> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12863> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15731> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15703> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12910> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12909> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15749> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12840> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15710> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12829> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15711> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15730> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12891> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15744> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12873> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15700> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12852> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12906> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15732> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12858> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15720> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15724> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15708> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12901> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12880> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12843> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12835> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12830> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12874> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15745> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15694> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12845> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12868> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12850> .
            <https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15741> .} }
            """
        AgnosticEntity._manage_update_queries(input_1, input_2)
        output = _to_nt_sorted_list(input_1)
        expected_output = [
            '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "1985-01-01"^^<http://www.w3.org/2001/XMLSchema#gYear>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/dc/terms/title> "Citation histories of scientific publications. The data sources"^^<http://www.w3.org/2001/XMLSchema#string>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15658>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15659>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15660>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15661>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15662>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15663>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15664>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15665>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15666>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15667>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15668>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15669>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15670>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15671>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15672>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15673>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15674>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15675>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15676>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15677>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15678>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15679>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15680>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15681>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15682>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15683>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15684>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15685>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15686>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15687>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15688>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15689>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15690>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15691>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15692>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15693>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15694>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15695>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15696>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15697>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15698>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15699>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15700>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15701>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15702>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15703>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15704>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15705>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15706>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15707>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15708>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15709>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15710>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15711>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15712>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15713>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15714>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15715>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15716>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15717>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15718>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15719>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15720>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15721>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15722>', 
            '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15723>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15724>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15725>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15726>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15727>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15728>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15729>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15730>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15731>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15732>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15733>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15734>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15735>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15736>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15737>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15738>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15739>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15740>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15741>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15742>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15743>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15744>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15745>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15746>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15747>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15748>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15749>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15750>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15751>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15752>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15753>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15754>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15755>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15756>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15757>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15758>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/13890>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/pro/isDocumentContextFor> <https://github.com/arcangelo7/time_agnostic/ar/2995>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#embodiment> <https://github.com/arcangelo7/time_agnostic/re/2015>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12821>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12822>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12823>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12824>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12825>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12826>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12827>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12828>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12829>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12830>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12831>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12832>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12833>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12834>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12835>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12836>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12837>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12838>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12839>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12840>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12841>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12842>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12843>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12844>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12845>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12846>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12847>', 
            '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12848>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12849>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12850>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12851>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12852>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12853>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12854>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12855>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12856>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12857>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12858>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12859>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12860>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12861>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12862>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12863>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12864>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12865>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12866>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12867>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12868>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12869>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12870>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12871>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12872>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12873>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12874>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12875>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12876>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12877>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12878>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12879>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12880>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12881>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12882>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12883>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12884>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12885>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12886>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12887>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12888>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12889>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12890>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12891>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12892>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12893>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12894>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12895>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12896>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12897>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12898>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12899>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12900>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12901>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12902>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12903>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12904>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12905>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12906>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12907>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12908>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12909>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12910>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12911>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12912>', 
            '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12913>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12914>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12915>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12916>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12917>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12918>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12919>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12920>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12921>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#partOf> <https://github.com/arcangelo7/time_agnostic/br/15657>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalArticle>'
        ]
        self.assertEqual(output, expected_output)

    def test__manage_update_queries_if_short_query(self):
        input_1 = ConjunctiveGraph()
        input_1.add((
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/id/1'), 
            rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), 
            rdflib.term.Literal('10.1007/bf02028088', datatype=rdflib.term.URIRef('http://www.w3.org/2001/XMLSchema#string')), 
            rdflib.term.URIRef("https://github.com/arcangelo7/time_agnostic/id/")
        ))
        input_2 = """
            DELETE DATA { 
                GRAPH <https://github.com/arcangelo7/time_agnostic/id/> { 
                    <https://github.com/arcangelo7/time_agnostic/id/1> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "10.1007/bf02028087"^^<http://www.w3.org/2001/XMLSchema#string> .
                } 
            };
            INSERT DATA { 
                GRAPH <https://github.com/arcangelo7/time_agnostic/id/> { 
                    <https://github.com/arcangelo7/time_agnostic/id/1> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "10.1007/bf02028088"^^<http://www.w3.org/2001/XMLSchema#string> .
                } 
            }
        """
        AgnosticEntity._manage_update_queries(input_1, input_2)
        output = _to_nt_sorted_list(input_1)
        expected_output = ['<https://github.com/arcangelo7/time_agnostic/id/1> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "10.1007/bf02028087"^^<http://www.w3.org/2001/XMLSchema#string>']
        self.assertEqual(output, expected_output)

    def test_manage_update_queries_if_multiple_delete_update(self):
        input_1 = ConjunctiveGraph()
        input_2 = """
            INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/br/> { <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78745> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78741> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "2017-01-01"^^<http://www.w3.org/2001/XMLSchema#date> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78768> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/pro/isDocumentContextFor> <https://github.com/arcangelo7/time_agnostic/ar/15393> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78732> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78748> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78764> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92952> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78737> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92933> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78733> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78762> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78730> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/pro/isDocumentContextFor> <https://github.com/arcangelo7/time_agnostic/ar/15394> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#embodiment> <https://github.com/arcangelo7/time_agnostic/re/10322> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/109427> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78761> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/1110> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/89955> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/109551> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/116915> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92796> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78735> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78756> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/42269> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78755> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92944> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78751> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78734> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/41421> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78740> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92772> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92945> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78753> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#embodiment> <https://github.com/arcangelo7/time_agnostic/re/10323> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/1142> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78746> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/112993> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/106670> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#partOf> <https://github.com/arcangelo7/time_agnostic/br/92918> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78743> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78729> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78731> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/46543> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/7870> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/dc/terms/title> "Filling the citation gap: measuring the multidimensional impact of the academic book at institutional level with PlumX"^^<http://www.w3.org/2001/XMLSchema#string> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/72623> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92922> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78760> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/121112> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/11088> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/11044> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/109550> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92943> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78736> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78747> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78759> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/62927> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78738> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/59410> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78754> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/119163> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/103953> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/48362> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78739> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/62850> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/pro/isDocumentContextFor> <https://github.com/arcangelo7/time_agnostic/ar/15392> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78765> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78750> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/120454> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/120259> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/2435> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78766> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78749> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92935> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#embodiment> <https://github.com/arcangelo7/time_agnostic/re/10324> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78744> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/59732> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78742> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/42274> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78757> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/41419> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78758> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78752> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78763> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/119259> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/42268> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/78767> .} };INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/br/> { <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/120456> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92946> .} }; DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/br/> { <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/120237> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/121112> .} };INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/br/> { <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/117087> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92912> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/5088> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92857> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92768> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/92788> .} }; DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/br/> { <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/109551> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/109427> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/120454> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/109550> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/116915> .
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/120456> .} };
        """
        AgnosticEntity._manage_update_queries(input_1, input_2)
        output = _to_nt_sorted_list(input_1)
        expected_output = ['<https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/109427>', '<https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/109550>', '<https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/109551>', '<https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/116915>', '<https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/120237>', '<https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/120454>', '<https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/120456>', '<https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/121112>']
        self.assertEqual(output, expected_output)

    def test__query_dataset(self):
        input = AgnosticEntity("https://github.com/arcangelo7/time_agnostic/ar/15519")
        output = _to_nt_sorted_list(input._query_dataset())
        expected_output = [
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
        ]
        self.assertEqual(output, expected_output)
    
    def test__query_provenance(self):
        input = AgnosticEntity("https://github.com/arcangelo7/time_agnostic/ra/15519")
        output = _to_nt_sorted_list(input._query_provenance())
        expected_output = [
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-07T09:59:15+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>', 
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/2> <http://www.w3.org/ns/prov#generatedAtTime> "2021-06-01T18:46:41+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>', 
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/2> <https://w3id.org/oc/ontology/hasUpdateQuery> "DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ra/> { <https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> \\\\"Giulio Marini\\\\"^^<http://www.w3.org/2001/XMLSchema#string> .\\\\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> \\\\"Giulio\\\\"^^<http://www.w3.org/2001/XMLSchema#string> .\\\\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509> .\\\\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> \\\\"Marini\\\\"^^<http://www.w3.org/2001/XMLSchema#string> .\\\\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent> .} }"^^<http://www.w3.org/2001/XMLSchema#string>'
        ]
        self.assertEqual(output, expected_output)

    def test__convert_to_datetime(self):
        input = "2021-05-21T19:08:56+00:00"
        expected_output = datetime.datetime(2021, 5, 21, 19, 8, 56)
        self.assertEqual(AgnosticEntity._convert_to_datetime(input), expected_output)


class Test_AgnosticQuery(unittest.TestCase):        
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
        AgnosticQuery(query)._tree_traverse(input1, input2, output)
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
        AgnosticQuery(query)._tree_traverse(input1, input2, output)
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
        AgnosticQuery(query)._tree_traverse(input1, input2, output)
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
        output = AgnosticQuery(input)._process_query()
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
            AgnosticQuery(input)._process_query()

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
        agnostic_query = AgnosticQuery(query)
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
        agnostic_query = AgnosticQuery(query)
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
        agnostic_query = AgnosticQuery(query)
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
        expected_reconstructed_entities = {rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519')}
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
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"^^<http://www.w3.org/2001/XMLSchema#string>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"^^<http://www.w3.org/2001/XMLSchema#string>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"^^<http://www.w3.org/2001/XMLSchema#string>'
                ]
            }
        }
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
        agnostic_query = AgnosticQuery(query)
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
        expected_reconstructed_entities = {rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/id/85509'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/4'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/id/14')}
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
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"^^<http://www.w3.org/2001/XMLSchema#string>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"^^<http://www.w3.org/2001/XMLSchema#string>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"^^<http://www.w3.org/2001/XMLSchema#string>'
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
        agnostic_query._solve_variables()
        pprint(agnostic_query.vars_to_explicit_by_time)
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
        agnostic_query = AgnosticQuery(query)
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
        agnostic_query = AgnosticQuery(query)
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
        agnostic_query = AgnosticQuery(query)
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
        agnostic_query = AgnosticQuery(query)
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
        agnostic_query = AgnosticQuery(query)
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
        agnostic_query = AgnosticQuery(query)
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
        agnostic_query = AgnosticQuery(query)
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
        agnostic_query = AgnosticQuery(query)
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
        agnostic_query = AgnosticQuery(query)
        triple = (rdflib.term.Variable('id'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('value'))
        output = agnostic_query._is_isolated(triple)
        self.assertEqual(output, False)

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
        agnostic_query = AgnosticQuery(query)
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
        agnostic_query = AgnosticQuery(query)
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

    def test__get_query_to_identify(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                ?a literal:hasLiteralValue ?b.
            }
        """
        triple = (rdflib.term.Variable('a'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('b'))
        query_to_identify = AgnosticQuery(query, entity_types={"http://purl.org/spar/datacite/Identifier"})._get_query_to_identify(triple).replace(" ", "").replace("\n", "")
        expected_query_to_identify = """
            SELECT DISTINCT ?updateQuery 
            WHERE {
                ?snapshot <https://w3id.org/oc/ontology/hasUpdateQuery> ?updateQuery.
                {?snapshot <http://www.w3.org/ns/prov#specializationOf>/a <http://purl.org/spar/datacite/Identifier>.}
                FILTER CONTAINS (?updateQuery, '<http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue>')
            }
        """.replace(" ", "").replace("\n", "")
        self.assertAlmostEqual(query_to_identify, expected_query_to_identify)

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
        agnostic_query = AgnosticQuery(query)
        agnostic_query.relevant_entities_graphs = dict()
        agnostic_query.reconstructed_entities = set()
        triple = (rdflib.term.Variable('a'), rdflib.term.URIRef('http://purl.org/spar/pro/isHeldBy'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'))
        extected_relevant_entities_graphs = {
            rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519'): {
                '2021-06-01T18:46:41': [], 
                '2021-05-07T09:59:15': [
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"^^<http://www.w3.org/2001/XMLSchema#string>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"^^<http://www.w3.org/2001/XMLSchema#string>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"^^<http://www.w3.org/2001/XMLSchema#string>'
                ]
            }, 
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
        expected_reconstructed_entities = {rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ar/15519'), rdflib.term.URIRef('https://github.com/arcangelo7/time_agnostic/ra/15519')}
        agnostic_query._find_entities_in_update_queries(triple)
        assert (agnostic_query.reconstructed_entities, _to_dict_of_nt_sorted_lists(agnostic_query.relevant_entities_graphs)) == (expected_reconstructed_entities, extected_relevant_entities_graphs)
    
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
        agnostic_query = AgnosticQuery(query)
        output = agnostic_query.run_agnostic_query()
        expected_output = {'2021-05-07T09:59:15': set(), '2021-05-31T18:19:47': {('https://github.com/arcangelo7/time_agnostic/ra/15519',)}, '2021-06-01T18:46:41': {('https://github.com/arcangelo7/time_agnostic/ra/4',)}}
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
        agnostic_query = AgnosticQuery(query)
        output = agnostic_query.run_agnostic_query()
        expected_output = {'2021-06-01T18:46:41': {('https://github.com/arcangelo7/time_agnostic/ra/4',)}, '2021-05-31T18:19:47': {('https://github.com/arcangelo7/time_agnostic/ra/15519',)}, '2021-05-07T09:59:15': {('https://github.com/arcangelo7/time_agnostic/ra/15519',)}}
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
        agnostic_query = AgnosticQuery(query, {"http://xmlns.com/foaf/0.1/Agent", "http://purl.org/spar/pro/RoleInTime"})
        output = agnostic_query.run_agnostic_query()
        expected_output = {
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
        }
        pprint(agnostic_query.vars_to_explicit_by_time)
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
        agnostic_query = AgnosticQuery(query)
        output = agnostic_query.run_agnostic_query()
        expected_output = {
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519', 'https://github.com/arcangelo7/time_agnostic/id/14')
            },
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519', 'https://github.com/arcangelo7/time_agnostic/id/14')
            },
            '2021-06-01T18:46:41': {
                ('None', 'https://github.com/arcangelo7/time_agnostic/id/14')
            }
        }
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
        agnostic_query = AgnosticQuery(query, {"http://purl.org/spar/pro/RoleInTime", "http://xmlns.com/foaf/0.1/Agent"})
        output = agnostic_query.run_agnostic_query()
        expected_output = {
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','http://purl.org/spar/pro/isHeldBy'),
                ('https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1','http://www.w3.org/ns/prov#specializationOf')
            },
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','http://purl.org/spar/pro/isHeldBy'),
                ('https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1','http://www.w3.org/ns/prov#specializationOf')
            },
            '2021-06-01T18:46:41': {
                ('https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1','http://www.w3.org/ns/prov#specializationOf'),
                ('https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/2','http://www.w3.org/ns/prov#specializationOf')}}
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_subj_obj_var(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?s ?o
            WHERE {
                ?s pro:isHeldBy ?o.
            }        
        """
        agnostic_query = AgnosticQuery(query)
        output = agnostic_query.run_agnostic_query()
        expected_output = {
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/15519')
            },
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/15519')
            },
            '2021-06-01T18:46:41': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/4')
            }
        }
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
        agnostic_query = AgnosticQuery(query)
        output = agnostic_query.run_agnostic_query()
        expected_output = {
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
        }
        self.assertEqual(output, expected_output)


class Test_BlazegraphQuery(unittest.TestCase):
    def test__get_query_to_identify(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            SELECT DISTINCT ?br ?id ?value
            WHERE {
                ?a literal:hasLiteralValue ?b.
            }   
        """
        triple = (rdflib.term.Variable('a'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('b'))
        query_to_identify = BlazegraphQuery(query, config_path="./test/config.json")._get_query_to_identify(triple).replace(" ", "").replace("\n", "")
        expected_query_to_identify = """
            PREFIX bds: <http://www.bigdata.com/rdf/search#>
            SELECT DISTINCT ?updateQuery 
            WHERE {
                ?snapshot <https://w3id.org/oc/ontology/hasUpdateQuery> ?updateQuery.
                ?updateQuery bds:search '<http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue>'.
            }
        """.replace(" ", "").replace("\n", "")
        self.assertAlmostEqual(query_to_identify, expected_query_to_identify)

    def test_run_agnostic_query_subj_obj_var(self):
        query = """
            prefix cito: <http://purl.org/spar/cito/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            select distinct ?elt_1
            where {
                ?elt_1 datacite:hasIdentifier ?id_1;
                    datacite:hasIdentifier ?id_2.
                ?id_1 literal:hasLiteralValue ?literal_1.
                ?id_2 literal:hasLiteralValue ?literal_2.
                FILTER (?literal_1 != ?literal_2)
            }     
        """
        agnostic_query = BlazegraphQuery(query, entity_types={"http://purl.org/spar/fabio/JournalArticle"}, config_path="./test/config.json")
        output = agnostic_query.run_agnostic_query()
        expected_output = {
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/15519')
            },
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/15519')
            },
            '2021-06-01T18:46:41': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/4')
            }
        }
        self.assertEqual(output, expected_output)


class Test_Support(unittest.TestCase):
    def test_import_json(self):
        input = "./test/config.json"
        expected_output = {
            "dataset": {
                "triplestore_urls": ["http://localhost:9999/blazegraph/sparql"],
                "file_paths": []
            },
            "provenance": {
                "triplestore_urls": [],
                "file_paths": ["./test/prov.json"]
            },
            "blazegraph_full_text_search": "yes",
            "cache_triplestore_url": "http://localhost:19999/blazegraph/sparql"
        }
        self.assertEqual(FileManager(input).import_json(), expected_output)


class Test_Sparql(unittest.TestCase):
    def test_run_select_query(self):
        input = """
            SELECT ?p ?o
            WHERE {
                BIND (<https://github.com/arcangelo7/time_agnostic/ar/15519> AS ?s) 
                ?s ?p ?o
            }
        """
        output = Sparql(input, "./test/config.json").run_select_query()
        expected_output = {
            ('http://purl.org/spar/pro/isHeldBy', 'https://github.com/arcangelo7/time_agnostic/ra/4'), 
            ('http://purl.org/spar/pro/withRole', 'http://purl.org/spar/pro/author'), 
            ('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://purl.org/spar/pro/RoleInTime'), 
            ('https://w3id.org/oc/ontology/hasNext', 'https://github.com/arcangelo7/time_agnostic/ar/15520')
        }
        self.assertEqual(output, expected_output)
    
    def test__get_tuples_from_file(self):
        input_1 = f"""
            SELECT ?p ?o
            WHERE {{
                BIND (<https://github.com/arcangelo7/time_agnostic/id/14/prov/se/1> AS ?s) 
                ?s ?p ?o;
                    a <{ProvEntity.iri_entity}>.
            }}
        """
        output = Sparql(input_1, "./test/config.json")._get_tuples_from_files()
        expected_output = {
            ('http://www.w3.org/ns/prov#generatedAtTime', '2021-05-07T09:59:15.000Z'), 
            ('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://www.w3.org/ns/prov#Entity'), 
            ('http://purl.org/dc/terms/description', "The entity 'https://github.com/arcangelo7/time_agnostic/id/14' has been created."), 
            ('http://www.w3.org/ns/prov#specializationOf', 'https://github.com/arcangelo7/time_agnostic/id/14'), 
            ('http://www.w3.org/ns/prov#wasAttributedTo', 'https://orcid.org/0000-0002-8420-0696'), 
            ('http://www.w3.org/ns/prov#invalidatedAtTime', '2021-06-01T18:46:41.000Z')
        }
        self.assertEqual(output, expected_output)
    
    def test__get_tuples_from_triplestores(self):
        input_1 = """
            SELECT ?p ?o
            WHERE {
                BIND (<https://github.com/arcangelo7/time_agnostic/id/14> AS ?s) 
                ?s ?p ?o
            }
        """
        output = Sparql(input_1, "./test/config.json")._get_tuples_from_triplestores()
        expected_output = {
            ('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://purl.org/spar/datacite/Identifier'), 
            ('http://purl.org/spar/datacite/usesIdentifierScheme', 'http://purl.org/spar/datacite/orcid'), 
            ('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue', 'http://orcid.org/0000-0002-3259-2309')
        }
        self.assertEqual(output, expected_output)
    
    def test_run_construct_query(self):
        input = """
            SELECT ?s ?p ?o ?c
            WHERE {
                GRAPH ?c {?s ?p ?o}
                BIND (<https://github.com/arcangelo7/time_agnostic/id/14> as ?s)
            }           
        """
        output = _to_nt_sorted_list(Sparql(input, "./test/config.json").run_construct_query())
        expected_output = [
            '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
            '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
            '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>'
        ]
        self.assertEqual(output, expected_output)
    
    def test__get_graph_from_files(self):
        input_1= f"""
            CONSTRUCT {{
                ?snapshot <{ProvEntity.iri_generated_at_time}> ?t;      
                        <{ProvEntity.iri_has_update_query}> ?updateQuery.
            }} 
            WHERE {{
                ?snapshot <{ProvEntity.iri_specialization_of}> <https://github.com/arcangelo7/time_agnostic/ar/15519>;
                        <{ProvEntity.iri_generated_at_time}> ?t.
            OPTIONAL {{
                    ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
                }}   
            }}
        """       
        output = _to_nt_sorted_list(Sparql(input_1, "./test/config.json")._get_graph_from_files()) 
        expected_output = [
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-07T09:59:15.000Z"', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-31T18:19:47.000Z"', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <https://w3id.org/oc/ontology/hasUpdateQuery> "INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime> .} }"', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://www.w3.org/ns/prov#generatedAtTime> "2021-06-01T18:46:41.000Z"', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <https://w3id.org/oc/ontology/hasUpdateQuery> "DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }"'
        ]
        self.assertEqual(output, expected_output)
    
    def test__get_graph_from_triplestores(self):
        input = f"""
            CONSTRUCT {{
                <https://github.com/arcangelo7/time_agnostic/ra/4> ?p ?o.     
            }} 
            WHERE {{
                <https://github.com/arcangelo7/time_agnostic/ra/4> ?p ?o. 
            }}
        """       
        sparql = Sparql(input, "./test/config.json")
        output = _to_nt_sorted_list(sparql._get_graph_from_triplestores()) 
        expected_output = [
            '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
            '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
            '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"^^<http://www.w3.org/2001/XMLSchema#string>', 
            '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"^^<http://www.w3.org/2001/XMLSchema#string>', 
            '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"^^<http://www.w3.org/2001/XMLSchema#string>'
        ]
        self.assertEqual(output, expected_output)

    def test__cut_by_limit(self):
        input_1 = """
            SELECT ?p ?o
            WHERE {
                BIND (<https://github.com/arcangelo7/time_agnostic/id/1/prov/se/1> AS ?s)
                ?s ?p ?o
            }
            LIMIT 2
        """
        input_2 = [
            ('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://www.w3.org/ns/prov#Entity'), 
            ('http://purl.org/dc/terms/description', "The entity 'https://github.com/arcangelo7/time_agnostic/id/1' has been created."), 
            ('http://www.w3.org/ns/prov#wasAttributedTo', 'https://orcid.org/0000-0002-8420-0696'), 
            ('http://www.w3.org/ns/prov#specializationOf', 'https://github.com/arcangelo7/time_agnostic/id/1'), 
            ('http://www.w3.org/ns/prov#generatedAtTime', '2021-05-06T18:14:42')
        ]
        output = len(Sparql(input_1)._cut_by_limit(input_2))
        expected_output = 2
        self.assertEqual(output, expected_output)


if __name__ == '__main__':
    unittest.main()
