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

import unittest

from time_agnostic_library.agnostic_entity import AgnosticEntity
from time_agnostic_library.support import (_nt_list_to_quad_set,
                                           _to_dict_of_nt_sorted_lists,
                                           _to_nt_sorted_list)

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

class TestAgnosticEntityHelpers(unittest.TestCase):
    maxDiff = None

    def test__get_entity_current_state(self):
        input = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        raw = AgnosticEntity(input, config=CONFIG)._get_entity_current_state()[0]
        output = _to_dict_of_nt_sorted_lists(raw)
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-07T09:59:15+00:00"', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://www.w3.org/ns/prov#specializationOf> <https://github.com/arcangelo7/time_agnostic/ar/15519>',
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-31T18:19:47+00:00"', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#specializationOf> <https://github.com/arcangelo7/time_agnostic/ar/15519>',
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#wasDerivedFrom> <https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1>',
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <https://w3id.org/oc/ontology/hasUpdateQuery> "INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime> .} }"', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://www.w3.org/ns/prov#generatedAtTime> "2021-06-01T18:46:41+00:00"',
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://www.w3.org/ns/prov#specializationOf> <https://github.com/arcangelo7/time_agnostic/ar/15519>',
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://www.w3.org/ns/prov#wasDerivedFrom> <https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2>',
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <https://w3id.org/oc/ontology/hasUpdateQuery> "DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }"', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
                    '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
                ], 
                '2021-05-31T18:19:47+00:00': None,
                '2021-05-07T09:59:15+00:00': None}}
        self.assertEqual(output, expected_output)
    
    def test__old_graphs(self):
        input_1 = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        input_2 = AgnosticEntity(input_1, config=CONFIG)._get_entity_current_state()
        raw = AgnosticEntity(input_1, config=CONFIG)._get_old_graphs(input_2)[0]
        output = _to_dict_of_nt_sorted_lists(raw)
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
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
        }
        self.assertEqual(output, expected_output)

    def test__include_prov_metadata(self):
        triples_generated_at_time = [
            ('<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2>', '<http://www.w3.org/ns/prov#generatedAtTime>', '"2021-05-31T18:19:47+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>'),
            ('<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3>', '<http://www.w3.org/ns/prov#generatedAtTime>', '"2021-06-01T18:46:41+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>'),
            ('<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1>', '<http://www.w3.org/ns/prov#generatedAtTime>', '"2021-05-07T09:59:15+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>')
        ]
        current_state = _nt_list_to_quad_set([
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-07T09:59:15+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://www.w3.org/ns/prov#wasAttributedTo> <https://orcid.org/0000-0002-8420-0696>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://www.w3.org/ns/prov#hadPrimarySource> "http://api.crossref.org/journals/0138-9130"', 
            '''<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://purl.org/dc/terms/description> "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been created."''', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-31T18:19:47+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#wasAttributedTo> <https://orcid.org/0000-0002-8420-0696>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <https://w3id.org/oc/ontology/hasUpdateQuery> "INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime> .} }"^^<http://www.w3.org/2001/XMLSchema#string>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#hadPrimarySource> "http://api.crossref.org/journals/0138-9130"', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#wasDerivedFrom> <https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1>',
            '''<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://purl.org/dc/terms/description> "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified."''', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://www.w3.org/ns/prov#generatedAtTime> "2021-06-01T18:46:41+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://www.w3.org/ns/prov#wasAttributedTo> <https://orcid.org/0000-0002-8420-0696>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <https://w3id.org/oc/ontology/hasUpdateQuery> "DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }"^^<http://www.w3.org/2001/XMLSchema#string>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://www.w3.org/ns/prov#wasDerivedFrom> <https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2>',
            '''<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://purl.org/dc/terms/description> "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified."''', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
        ])
        agnostic_entity = AgnosticEntity("https://github.com/arcangelo7/time_agnostic/ar/15519", config=CONFIG)
        output = agnostic_entity._include_prov_metadata(triples_generated_at_time, current_state)
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2': {
                    'generatedAtTime': '2021-05-31T18:19:47+00:00', 
                    'invalidatedAtTime': None, 
                    'wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'hadPrimarySource': 'http://api.crossref.org/journals/0138-9130', 
                    'description': "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified.", 
                    'hasUpdateQuery': 'INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime> .} }',
                    'wasDerivedFrom': ['https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1']
                }, 
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3': {
                    'generatedAtTime': '2021-06-01T18:46:41+00:00', 
                    'invalidatedAtTime': None, 
                    'wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'hadPrimarySource': None, 
                    'description': "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified.", 
                    'hasUpdateQuery': 'DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }',
                    'wasDerivedFrom': ['https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2']
                }, 
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1': {
                    'generatedAtTime': '2021-05-07T09:59:15+00:00', 
                    'invalidatedAtTime': None, 
                    'wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'hadPrimarySource': 'http://api.crossref.org/journals/0138-9130', 
                    'description': "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been created.", 
                    'hasUpdateQuery': None,
                    'wasDerivedFrom': []
                }
            }
        }
        self.assertEqual(output, expected_output)

    def test__include_prov_metadata_if_prov_input(self):
        triples_generated_at_time = [
            ('<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1>', '<http://www.w3.org/ns/prov#generatedAtTime>', '"2021-05-07T09:59:15.000Z"')
        ]
        current_state = _nt_list_to_quad_set([
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1> <http://purl.org/dc/terms/description> "The entity \\\'https://github.com/arcangelo7/time_agnostic/ra/15519\\\' has been created."',
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Entity>',
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-07T09:59:15.000Z"',
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1> <http://www.w3.org/ns/prov#invalidatedAtTime> "2021-06-01T18:46:41.000Z"',
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1> <http://www.w3.org/ns/prov#specializationOf> <https://github.com/arcangelo7/time_agnostic/ra/15519>',
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1> <http://www.w3.org/ns/prov#wasAttributedTo> <https://orcid.org/0000-0002-8420-0696>'
        ])
        agnostic_entity = AgnosticEntity("https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1", config=CONFIG)
        output = agnostic_entity._include_prov_metadata(triples_generated_at_time, current_state)
        expected_output = dict()
        self.assertEqual(output, expected_output)

    def test__manage_long_update_queries_insert_data_only(self):
        input_1 = set()
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
                "1985-01-01" .
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
            '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://prismstandard.org/namespaces/basic/2.0/publicationDate> "1985-01-01"', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/dc/terms/title> "Citation histories of scientific publications. The data sources"', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15658>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15659>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15660>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15661>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15662>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15663>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15664>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15665>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15666>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15667>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15668>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15669>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15670>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15671>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15672>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15673>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15674>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15675>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15676>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15677>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15678>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15679>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15680>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15681>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15682>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15683>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15684>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15685>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15686>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15687>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15688>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15689>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15690>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15691>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15692>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15693>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15694>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15695>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15696>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15697>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15698>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15699>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15700>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15701>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15702>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15703>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15704>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15705>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15706>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15707>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15708>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15709>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15710>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15711>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15712>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15713>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15714>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15715>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15716>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15717>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15718>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15719>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15720>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15721>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15722>', 
            '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15723>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15724>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15725>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15726>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15727>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15728>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15729>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15730>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15731>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15732>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15733>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15734>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15735>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15736>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15737>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15738>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15739>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15740>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15741>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15742>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15743>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15744>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15745>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15746>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15747>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15748>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15749>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15750>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15751>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15752>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15753>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15754>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15755>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15756>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15757>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/cito/cites> <https://github.com/arcangelo7/time_agnostic/br/15758>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/13890>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/spar/pro/isDocumentContextFor> <https://github.com/arcangelo7/time_agnostic/ar/2995>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#embodiment> <https://github.com/arcangelo7/time_agnostic/re/2015>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12821>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12822>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12823>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12824>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12825>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12826>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12827>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12828>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12829>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12830>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12831>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12832>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12833>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12834>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12835>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12836>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12837>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12838>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12839>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12840>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12841>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12842>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12843>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12844>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12845>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12846>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12847>', 
            '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12848>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12849>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12850>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12851>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12852>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12853>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12854>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12855>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12856>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12857>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12858>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12859>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12860>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12861>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12862>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12863>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12864>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12865>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12866>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12867>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12868>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12869>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12870>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12871>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12872>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12873>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12874>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12875>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12876>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12877>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12878>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12879>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12880>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12881>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12882>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12883>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12884>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12885>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12886>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12887>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12888>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12889>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12890>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12891>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12892>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12893>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12894>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12895>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12896>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12897>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12898>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12899>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12900>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12901>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12902>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12903>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12904>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12905>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12906>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12907>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12908>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12909>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12910>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12911>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12912>', 
            '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12913>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12914>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12915>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12916>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12917>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12918>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12919>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12920>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#part> <https://github.com/arcangelo7/time_agnostic/be/12921>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://purl.org/vocab/frbr/core#partOf> <https://github.com/arcangelo7/time_agnostic/br/15657>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>', '<https://github.com/arcangelo7/time_agnostic/br/15655> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalArticle>'
        ]
        self.assertEqual(output, expected_output)

    def test__manage_update_queries_if_short_query(self):
        input_1 = {(
            '<https://github.com/arcangelo7/time_agnostic/id/1>',
            '<http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue>',
            '"10.1007/bf02028088"',
            '<https://github.com/arcangelo7/time_agnostic/id/>'
        )}
        input_2 = """
            DELETE DATA { 
                GRAPH <https://github.com/arcangelo7/time_agnostic/id/> { 
                    <https://github.com/arcangelo7/time_agnostic/id/1> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "10.1007/bf02028087" .
                } 
            };
            INSERT DATA { 
                GRAPH <https://github.com/arcangelo7/time_agnostic/id/> { 
                    <https://github.com/arcangelo7/time_agnostic/id/1> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "10.1007/bf02028088" .
                } 
            }
        """
        AgnosticEntity._manage_update_queries(input_1, input_2)
        output = _to_nt_sorted_list(input_1)
        expected_output = ['<https://github.com/arcangelo7/time_agnostic/id/1> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "10.1007/bf02028087"']
        self.assertEqual(output, expected_output)

    def test_manage_update_queries_if_multiple_delete_update(self):
        input_1 = set()
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
            <https://github.com/arcangelo7/time_agnostic/br/87430> <http://purl.org/dc/terms/title> "Filling the citation gap: measuring the multidimensional impact of the academic book at institutional level with PlumX" .
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
        input = AgnosticEntity("https://github.com/arcangelo7/time_agnostic/ar/15519", config=CONFIG)
        output = _to_nt_sorted_list(input._query_dataset())
        expected_output = [
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>'
        ]
        self.assertEqual(output, expected_output)
    
    def test__query_provenance(self):
        input = AgnosticEntity("https://github.com/arcangelo7/time_agnostic/ra/15519", config=CONFIG)
        output = _to_nt_sorted_list(input._query_provenance())
        expected_output = [
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-07T09:59:15+00:00"',
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1> <http://www.w3.org/ns/prov#specializationOf> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/2> <http://www.w3.org/ns/prov#generatedAtTime> "2021-06-01T18:46:41+00:00"',
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/2> <http://www.w3.org/ns/prov#specializationOf> <https://github.com/arcangelo7/time_agnostic/ra/15519>', 
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/2> <http://www.w3.org/ns/prov#wasDerivedFrom> <https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1>',
            '<https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/2> <https://w3id.org/oc/ontology/hasUpdateQuery> "DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ra/> { <https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> \\"Giulio Marini\\"^^<http://www.w3.org/2001/XMLSchema#string> .\\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> \\"Giulio\\"^^<http://www.w3.org/2001/XMLSchema#string> .\\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509> .\\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> \\"Marini\\"^^<http://www.w3.org/2001/XMLSchema#string> .\\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent> .} }\"'
        ]
        self.assertEqual(output, expected_output)

if __name__ == '__main__': # pragma: no cover
    unittest.main() 