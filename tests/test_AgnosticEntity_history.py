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
from time_agnostic_library.support import _to_dict_of_nt_sorted_lists

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
    "graphdb_connector_name": "",
    "cache_triplestore_url": {
        "endpoint": "",
        "update_endpoint": ""
    }
}

class TestAgnosticEntityHistory(unittest.TestCase):
    maxDiff = None

    def test_get_history(self):
        input = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        entity_history = AgnosticEntity(input, config=CONFIG).get_history()
        output = (_to_dict_of_nt_sorted_lists(entity_history[0]), entity_history[1])
        expected_output = ({
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
        }, None)
        self.assertEqual(output, expected_output)

    def test_get_history_with_metadata(self):
        input = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        output = AgnosticEntity(input, config=CONFIG).get_history(include_prov_metadata=True)
        output_0 = _to_dict_of_nt_sorted_lists(output[0])
        output_1 = output[1]
        expected_output_0 = {
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

        expected_output_1 = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3': {
                    'generatedAtTime': '2021-06-01T18:46:41+00:00', 
                    'invalidatedAtTime': None, 
                    'wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'hadPrimarySource': None, 
                    'description': "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified.", 
                    'hasUpdateQuery': 'DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }'}, 
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1': {
                    'generatedAtTime': '2021-05-07T09:59:15+00:00', 
                    'invalidatedAtTime': '2021-05-31T18:19:47+00:00', 
                    'wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'hadPrimarySource': None, 
                    'description': "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been created.", 
                    'hasUpdateQuery': None}, 
                'https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2': {
                    'generatedAtTime': '2021-05-31T18:19:47+00:00', 
                    'invalidatedAtTime': '2021-06-01T18:46:41+00:00', 
                    'wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'hadPrimarySource': None, 
                    'description': "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified.", 
                    'hasUpdateQuery': 'INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime> .} }'}}}

        self.assertEqual(output_0, expected_output_0)
        self.assertEqual(output_1, expected_output_1)
    
    def test_get_history_with_related_entities_author_role(self):
        input_uri = "https://github.com/arcangelo7/time_agnostic/ar/15519"
        entity = AgnosticEntity(input_uri, related_entities_history=True, config=CONFIG)
        history, prov_metadata = entity.get_history(include_prov_metadata=True)
        # Convert results to normalized format for comparison
        history_dict = _to_dict_of_nt_sorted_lists(history)
        # Expected snapshots for role entity
        expected_history = {
            "https://github.com/arcangelo7/time_agnostic/ar/15519": {
                "2021-05-07T09:59:15+00:00": [
                    "<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>",
                    "<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>",
                    "<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>",
                    "<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>",
                    "<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> \"http://orcid.org/0000-0002-3259-2309\"",
                    "<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>",
                    "<https://github.com/arcangelo7/time_agnostic/id/85509> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>",
                    "<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> \"http://orcid.org/0000-0002-3259-2309\"",
                    "<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> \"Marini\"",
                    "<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> \"Giulio\"",
                    "<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> \"Giulio Marini\"",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> \"Marini\"",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> \"Giulio\"",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> \"Giulio Marini\""
                ],
                "2021-05-31T18:19:47+00:00": [
                    "<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519>",
                    "<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>",
                    "<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>",
                    "<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>",
                    "<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>",
                    "<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> \"http://orcid.org/0000-0002-3259-2309\"",
                    "<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>",
                    "<https://github.com/arcangelo7/time_agnostic/id/85509> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>",
                    "<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> \"http://orcid.org/0000-0002-3259-2309\"",
                    "<https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> \"Marini\"",
                    "<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> \"Giulio\"",
                    "<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> \"Giulio Marini\"",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> \"Marini\"",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> \"Giulio\"",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> \"Giulio Marini\""
                ],
                "2021-06-01T18:46:41+00:00": [
                    "<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4>",
                    "<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/withRole> <http://purl.org/spar/pro/author>",
                    "<https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime>",
                    "<https://github.com/arcangelo7/time_agnostic/ar/15519> <https://w3id.org/oc/ontology/hasNext> <https://github.com/arcangelo7/time_agnostic/ar/15520>",
                    "<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>",
                    "<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> \"http://orcid.org/0000-0002-3259-2309\"",
                    "<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> \"Marini\"",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> \"Giulio\"",
                    "<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> \"Giulio Marini\""
                ]
            }
        }
        
        self.assertEqual(history_dict, expected_history)

        expected_prov_metadata = {
            "https://github.com/arcangelo7/time_agnostic/ar/15519": {
                "https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2": {
                    "generatedAtTime": "2021-05-31T18:19:47+00:00",
                    "invalidatedAtTime": "2021-06-01T18:46:41+00:00",
                    "wasAttributedTo": "https://orcid.org/0000-0002-8420-0696",
                    "hadPrimarySource": None,
                    "description": "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified.",
                    "hasUpdateQuery": "INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime> .} }"
                },
                "https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1": {
                    "generatedAtTime": "2021-05-07T09:59:15+00:00",
                    "invalidatedAtTime": "2021-05-31T18:19:47+00:00",
                    "wasAttributedTo": "https://orcid.org/0000-0002-8420-0696",
                    "hadPrimarySource": None,
                    "description": "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been created.",
                    "hasUpdateQuery": None
                },
                "https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3": {
                    "generatedAtTime": "2021-06-01T18:46:41+00:00",
                    "invalidatedAtTime": None,
                    "wasAttributedTo": "https://orcid.org/0000-0002-8420-0696",
                    "hadPrimarySource": None,
                    "description": "The entity 'https://github.com/arcangelo7/time_agnostic/ar/15519' has been modified.",
                    "hasUpdateQuery": "DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }"
                }
            },
            "https://github.com/arcangelo7/time_agnostic/ra/15519": {
                "https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/2": {
                    "generatedAtTime": "2021-06-01T18:46:41+00:00",
                    "invalidatedAtTime": "2021-06-01T18:46:41+00:00",
                    "wasAttributedTo": "https://orcid.org/0000-0002-8420-0696",
                    "hadPrimarySource": None,
                    "description": "The entity 'https://github.com/arcangelo7/time_agnostic/ra/15519' has been deleted.",
                    "hasUpdateQuery": "DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ra/> { <https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> \"Giulio Marini\"^^<http://www.w3.org/2001/XMLSchema#string> .\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> \"Giulio\"^^<http://www.w3.org/2001/XMLSchema#string> .\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509> .\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> \"Marini\"^^<http://www.w3.org/2001/XMLSchema#string> .\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent> .} }"
                },
                "https://github.com/arcangelo7/time_agnostic/ra/15519/prov/se/1": {
                    "generatedAtTime": "2021-05-07T09:59:15+00:00",
                    "invalidatedAtTime": "2021-06-01T18:46:41+00:00",
                    "wasAttributedTo": "https://orcid.org/0000-0002-8420-0696",
                    "hadPrimarySource": None,
                    "description": "The entity 'https://github.com/arcangelo7/time_agnostic/ra/15519' has been created.",
                    "hasUpdateQuery": None
                }
            },
            "https://github.com/arcangelo7/time_agnostic/id/85509": {
                "https://github.com/arcangelo7/time_agnostic/id/85509/prov/se/1": {
                    "generatedAtTime": "2021-05-07T09:59:15+00:00",
                    "invalidatedAtTime": "2021-06-01T18:46:41+00:00",
                    "wasAttributedTo": "https://orcid.org/0000-0002-8420-0696",
                    "hadPrimarySource": None,
                    "description": "The entity 'https://github.com/arcangelo7/time_agnostic/id/85509' has been created.",
                    "hasUpdateQuery": None
                },
                "https://github.com/arcangelo7/time_agnostic/id/85509/prov/se/2": {
                    "generatedAtTime": "2021-06-01T18:46:41+00:00",
                    "invalidatedAtTime": "2021-06-01T18:46:41+00:00",
                    "wasAttributedTo": "https://orcid.org/0000-0002-8420-0696",
                    "hadPrimarySource": None,
                    "description": "The entity 'https://github.com/arcangelo7/time_agnostic/id/85509' has been deleted.",
                    "hasUpdateQuery": "DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/id/> { <https://github.com/arcangelo7/time_agnostic/id/85509> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid> . <https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> 'http://orcid.org/0000-0002-3259-2309'. <https://github.com/arcangelo7/time_agnostic/id/85509> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier> . } }"
                }
            },
            "https://github.com/arcangelo7/time_agnostic/ra/4": {
                "https://github.com/arcangelo7/time_agnostic/ra/4/prov/se/1": {
                    "generatedAtTime": "2021-05-07T09:59:15+00:00",
                    "invalidatedAtTime": "2021-06-01T18:46:41+00:00",
                    "wasAttributedTo": "https://orcid.org/0000-0002-8420-0696",
                    "hadPrimarySource": None,
                    "description": "The entity 'https://github.com/arcangelo7/time_agnostic/ra/4' has been created.",
                    "hasUpdateQuery": None
                },
                "https://github.com/arcangelo7/time_agnostic/ra/4/prov/se/2": {
                    "generatedAtTime": "2021-06-01T18:46:41+00:00",
                    "invalidatedAtTime": None,
                    "wasAttributedTo": "https://orcid.org/0000-0002-8420-0696",
                    "hadPrimarySource": None,
                    "description": "The entity 'https://github.com/arcangelo7/time_agnostic/ra/4' has been merged with 'https://github.com/arcangelo7/time_agnostic/ra/15519'.",
                    "hasUpdateQuery": None
                }
            },
            "https://github.com/arcangelo7/time_agnostic/id/14": {
                "https://github.com/arcangelo7/time_agnostic/id/14/prov/se/1": {
                    "generatedAtTime": "2021-05-07T09:59:15+00:00",
                    "invalidatedAtTime": "2021-06-01T18:46:41+00:00",
                    "wasAttributedTo": "https://orcid.org/0000-0002-8420-0696",
                    "hadPrimarySource": None,
                    "description": "The entity 'https://github.com/arcangelo7/time_agnostic/id/14' has been created.",
                    "hasUpdateQuery": None
                },
                "https://github.com/arcangelo7/time_agnostic/id/14/prov/se/2": {
                    "generatedAtTime": "2021-06-01T18:46:41+00:00",
                    "invalidatedAtTime": None,
                    "wasAttributedTo": "https://orcid.org/0000-0002-8420-0696",
                    "hadPrimarySource": None,
                    "description": "The entity 'https://github.com/arcangelo7/time_agnostic/id/14' has been merged with 'https://github.com/arcangelo7/time_agnostic/id/85509'.",
                    "hasUpdateQuery": None
                }
            }
        }
        self.assertEqual(prov_metadata, expected_prov_metadata)

    def test_get_history_and_related_entities(self):
        input = "https://github.com/arcangelo7/time_agnostic/ra/4"
        output = AgnosticEntity(input, related_entities_history=True, config=CONFIG).get_history(include_prov_metadata=False)
        output_0 = _to_dict_of_nt_sorted_lists(output[0])
        expected_output_0 = {
            'https://github.com/arcangelo7/time_agnostic/ra/4': {
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>',
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> \"http://orcid.org/0000-0002-3259-2309\"',
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>',
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>',
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> \"http://orcid.org/0000-0002-3259-2309\"',
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>',
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"']
            }
        }
        self.assertEqual(output_0, expected_output_0)

    def test_get_history_and_related_entities_with_metadata(self):
        input = "https://github.com/arcangelo7/time_agnostic/ra/4"
        output = AgnosticEntity(input, related_entities_history=True, config=CONFIG).get_history(include_prov_metadata=True)
        output_0 = _to_dict_of_nt_sorted_lists(output[0])
        output_1 = output[1]
        expected_output_0 = {
            'https://github.com/arcangelo7/time_agnostic/ra/4': {
                '2021-06-01T18:46:41+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>',
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"'
                ], 
                '2021-05-07T09:59:15+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://purl.org/spar/datacite/usesIdentifierScheme> <http://purl.org/spar/datacite/orcid>', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "http://orcid.org/0000-0002-3259-2309"', 
                    '<https://github.com/arcangelo7/time_agnostic/id/14> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/datacite/Identifier>',
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/14>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent>', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/familyName> "Marini"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/givenName> "Giulio"', 
                    '<https://github.com/arcangelo7/time_agnostic/ra/4> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"']
            }, 
        }

        expected_output_1 = {
            'https://github.com/arcangelo7/time_agnostic/ra/4': {
                'https://github.com/arcangelo7/time_agnostic/ra/4/prov/se/1': {
                    'generatedAtTime': '2021-05-07T09:59:15+00:00', 
                    'invalidatedAtTime': '2021-06-01T18:46:41+00:00', 
                    'wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'hadPrimarySource': None, 
                    'description': "The entity 'https://github.com/arcangelo7/time_agnostic/ra/4' has been created.", 
                    'hasUpdateQuery': None}, 
                'https://github.com/arcangelo7/time_agnostic/ra/4/prov/se/2': {
                    'generatedAtTime': '2021-06-01T18:46:41+00:00', 
                    'invalidatedAtTime': None, 
                    'wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'hadPrimarySource': None, 
                    'description': "The entity 'https://github.com/arcangelo7/time_agnostic/ra/4' has been merged with 'https://github.com/arcangelo7/time_agnostic/ra/15519'.", 
                    'hasUpdateQuery': None}}, 
            'https://github.com/arcangelo7/time_agnostic/id/14': {
                'https://github.com/arcangelo7/time_agnostic/id/14/prov/se/1': {
                    'generatedAtTime': '2021-05-07T09:59:15+00:00', 
                    'invalidatedAtTime': '2021-06-01T18:46:41+00:00', 
                    'wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'hadPrimarySource': None, 
                    'description': "The entity 'https://github.com/arcangelo7/time_agnostic/id/14' has been created.", 
                    'hasUpdateQuery': None
                }, 
                'https://github.com/arcangelo7/time_agnostic/id/14/prov/se/2': {
                    'generatedAtTime': '2021-06-01T18:46:41+00:00', 
                    'invalidatedAtTime': None, 
                    'wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696', 
                    'hadPrimarySource': None, 
                    'description': "The entity 'https://github.com/arcangelo7/time_agnostic/id/14' has been merged with 'https://github.com/arcangelo7/time_agnostic/id/85509'.", 
                    'hasUpdateQuery': None
                }
            }
        }
        self.assertEqual(output_0, expected_output_0)
        self.assertEqual(output_1, expected_output_1)


if __name__ == '__main__': # pragma: no cover
    unittest.main() 