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


from datetime import datetime
from SPARQLWrapper import SPARQLWrapper, JSON
from time_agnostic_library.support import empty_the_cache, convert_to_datetime
import unittest, json


CONFIG_PATH = "tests/config.json"
CONFIG_GRAPHDB = "tests/config_graphdb.json"
CONFIG_FUSEKI= "tests/config_fuseki.json"
CONFIG_VIRTUOSO= "tests/config_virtuoso.json"

class Test_Support(unittest.TestCase):
    def test_empty_the_cache(self):
        empty_the_cache(CONFIG_PATH)
        with open(CONFIG_PATH, encoding="utf8") as json_file:
            cache_triplestore_url = json.load(json_file)["cache_triplestore_url"]["update_endpoint"]
        if cache_triplestore_url:
            sparql = SPARQLWrapper(cache_triplestore_url)
            query = "select ?g where {GRAPH ?g {?s ?p ?o}}"
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.queryAndConvert()
            expected_results = {'head': {'vars': ['g']}, 'results': {'bindings': []}}
            self.assertEqual(results, expected_results)

    def test_convert_to_datetime(self):
        input = "2021-05-21T19:08:56+00:00"
        expected_output = datetime(2021, 5, 21, 19, 8, 56)
        self.assertEqual(convert_to_datetime(input), expected_output)



if __name__ == '__main__': # pragma: no cover
    unittest.main()
