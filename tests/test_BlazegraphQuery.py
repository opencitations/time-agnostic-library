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

import unittest, rdflib
from time_agnostic_library.agnostic_query import BlazegraphQuery

CONFIG_PATH = "tests/config.json"

class Test_BlazegraphQuery(unittest.TestCase):
    def test__get_query_to_update_queries(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            SELECT DISTINCT ?br ?id ?value
            WHERE {
                ?a literal:hasLiteralValue ?b.
            }   
        """
        triple = (rdflib.term.Variable('a'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('b'))
        query_to_identify = BlazegraphQuery(query, config_path=CONFIG_PATH)._get_query_to_update_queries(triple).replace(" ", "").replace("\n", "")
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
