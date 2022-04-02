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
from pprint import pprint
from time_agnostic_library.sparql import Sparql
from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.support import _to_nt_sorted_list

CONFIG_PATH = "tests/config.json"

class Test_Sparql(unittest.TestCase):
    def test_run_select_query(self):
        input = """
            SELECT ?p ?o
            WHERE {
                BIND (<https://github.com/arcangelo7/time_agnostic/ar/15519> AS ?s) 
                ?s ?p ?o
            }
        """
        output = Sparql(input, CONFIG_PATH).run_select_query()
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
        output = Sparql(input_1, CONFIG_PATH)._get_tuples_from_files()
        expected_output = {
            ('http://www.w3.org/ns/prov#generatedAtTime', '2021-05-07T09:59:15+00:00'), 
            ('http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 'http://www.w3.org/ns/prov#Entity'), 
            ('http://purl.org/dc/terms/description', "The entity 'https://github.com/arcangelo7/time_agnostic/id/14' has been created."), 
            ('http://www.w3.org/ns/prov#specializationOf', 'https://github.com/arcangelo7/time_agnostic/id/14'), 
            ('http://www.w3.org/ns/prov#wasAttributedTo', 'https://orcid.org/0000-0002-8420-0696'), 
            ('http://www.w3.org/ns/prov#invalidatedAtTime', '2021-06-01T18:46:41+00:00')
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
        output = Sparql(input_1, CONFIG_PATH)._get_tuples_from_triplestores()
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
                BIND (<https://github.com/arcangelo7/time_agnostic/id/14> as ?s)
                GRAPH ?c {?s ?p ?o}
            }           
        """
        output = _to_nt_sorted_list(Sparql(input, CONFIG_PATH).run_construct_query())
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
        output = _to_nt_sorted_list(Sparql(input_1, CONFIG_PATH)._get_graph_from_files()) 
        expected_output = [
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/1> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-07T09:59:15+00:00"', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <http://www.w3.org/ns/prov#generatedAtTime> "2021-05-31T18:19:47+00:00"', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/2> <https://w3id.org/oc/ontology/hasUpdateQuery> "INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/pro/RoleInTime> .} }"', 
            '<https://github.com/arcangelo7/time_agnostic/ar/15519/prov/se/3> <http://www.w3.org/ns/prov#generatedAtTime> "2021-06-01T18:46:41+00:00"', 
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
        sparql = Sparql(input, CONFIG_PATH)
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
