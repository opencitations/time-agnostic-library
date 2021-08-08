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

import unittest, datetime, rdflib, json

from rdflib.graph import ConjunctiveGraph
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from pprint import pprint

from time_agnostic_library.sparql import Sparql
from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.agnostic_entity import AgnosticEntity
from time_agnostic_library.agnostic_query import AgnosticQuery, BlazegraphQuery
from time_agnostic_library.support import _to_dict_of_nt_sorted_lists, _to_nt_sorted_list, _to_dict_of_conjunctive_graphs, _to_conjunctive_graph, empty_the_cache

CONFIG_PATH = "test/config.json"

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
        query_to_identify = BlazegraphQuery(query, config_path=CONFIG_PATH)._get_query_to_identify(triple).replace(" ", "").replace("\n", "")
        expected_query_to_identify = """
            PREFIX bds: <http://www.bigdata.com/rdf/search#>
            SELECT DISTINCT ?updateQuery 
            WHERE {
                ?snapshot <https://w3id.org/oc/ontology/hasUpdateQuery> ?updateQuery.
                ?updateQuery bds:search '<http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue>'.
            }
        """.replace(" ", "").replace("\n", "")
        self.assertAlmostEqual(query_to_identify, expected_query_to_identify)

    # def test_run_agnostic_query_subj_obj_var(self):
    #     query = """
    #         prefix cito: <http://purl.org/spar/cito/>
    #         prefix datacite: <http://purl.org/spar/datacite/>
    #         prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
    #         select distinct ?elt_1
    #         where {
    #             ?elt_1 datacite:hasIdentifier ?id_1;
    #                 datacite:hasIdentifier ?id_2.
    #             ?id_1 literal:hasLiteralValue ?literal_1.
    #             ?id_2 literal:hasLiteralValue ?literal_2.
    #             FILTER (?literal_1 != ?literal_2)
    #         }     
    #     """
    #     agnostic_query = BlazegraphQuery(query, entity_types={"http://purl.org/spar/fabio/JournalArticle"}, config_path=CONFIG_PATH)
    #     output = agnostic_query.run_agnostic_query()
    #     expected_output = {
    #         '2021-05-07T09:59:15': {
    #             ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/15519')
    #         },
    #         '2021-05-31T18:19:47': {
    #             ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/15519')
    #         },
    #         '2021-06-01T18:46:41': {
    #             ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/4')
    #         }
    #     }
    #     self.assertEqual(output, expected_output)


if __name__ == '__main__':
    unittest.main()
