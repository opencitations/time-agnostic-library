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


from pprint import pprint
from time_agnostic_library.agnostic_query import VersionQuery
from time_agnostic_library.statistics import Statistics
import os
import unittest

CONFIG_PATH = os.path.join('tests', 'config.json')
CONFIG_BLAZEGRAPH = os.path.join('tests', 'config_blazegraph.json')

class Test_Statistics(unittest.TestCase):
    def test_calculcate_statistics(self):
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
        statistics = Statistics(agnostic_query.relevant_entities_graphs)
        number_of_entities = statistics.get_number_of_entities()
        number_of_snapshots = statistics.get_number_of_snapshots()
        output = (number_of_entities, number_of_snapshots)
        expected_output = (3,7)
        self.assertEqual(output, expected_output)
