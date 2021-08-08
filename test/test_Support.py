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

class Test_Support(unittest.TestCase):
    def test_empty_the_cache(self):
        empty_the_cache(CONFIG_PATH)
        with open(CONFIG_PATH, encoding="utf8") as json_file:
            cache_triplestore_url = json.load(json_file)["cache_triplestore_url"]
        if cache_triplestore_url:
            sparql = SPARQLWrapper(cache_triplestore_url)
            query = "select ?g where {GRAPH ?g {?s ?p ?o}}"
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.queryAndConvert()
            expected_results = {'head': {'vars': ['g']}, 'results': {'bindings': []}}
            self.assertEqual(results, expected_results)


if __name__ == '__main__':
    unittest.main()
