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


from time_agnostic_library.agnostic_query import VersionQuery, AgnosticEntity, DeltaQuery
from time_agnostic_library.statistics import Statistics
import os
import unittest

CONFIG_PATH = os.path.join('tests', 'config.json')
CONFIG = {
    "dataset": {
        "triplestore_urls": ["http://127.0.0.1:9999/blazegraph/sparql"],
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
CONFIG_BLAZEGRAPH = os.path.join('tests', 'config_blazegraph.json')

class Test_Statistics(unittest.TestCase):
    def test_statistics_cv(self):
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
        overhead = statistics.get_overhead()
        expected_output = 3
        self.assertEqual(overhead, expected_output)

    def test_statistics_sv(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                    rdf:type pro:RoleInTime.
            }
        """
        agnostic_query = VersionQuery(query, on_time=('2021-05-31T18:19:47', '2021-05-31T18:19:47'), config_path=CONFIG_PATH)
        agnostic_query.run_agnostic_query()
        statistics = Statistics(agnostic_query.relevant_entities_graphs)
        overhead = statistics.get_overhead()
        expected_output = 1
        self.assertEqual(overhead, expected_output)

    def test_statistics_ma_all(self):
        agnostic_entity = AgnosticEntity('https://github.com/arcangelo7/time_agnostic/ar/15519', config=CONFIG)
        entity_history, _ = agnostic_entity.get_history()
        statistics = Statistics(entity_history)
        overhead = statistics.get_overhead()
        expected_output = 3
        self.assertEqual(overhead, expected_output)

    def test_statistics_ma_single(self):
        agnostic_entity = AgnosticEntity('https://github.com/arcangelo7/time_agnostic/ar/15519', config=CONFIG)
        _, entity_snapshots, other_snapshots = agnostic_entity.get_state_at_time(time=('2021-05-07T09:59:15', '2021-05-07T09:59:15'), include_prov_metadata=True)
        statistics = Statistics((entity_snapshots, other_snapshots))
        overhead = statistics.get_overhead()
        expected_output = 3
        self.assertEqual(overhead, expected_output)

    def test_statistics_ma_single_2(self):
        agnostic_entity = AgnosticEntity('https://github.com/arcangelo7/time_agnostic/ar/15519', config=CONFIG)
        _, entity_snapshots, other_snapshots = agnostic_entity.get_state_at_time(time=('2021-05-31T18:19:47', '2021-05-31T18:19:47'), include_prov_metadata=True)
        statistics = Statistics((entity_snapshots, other_snapshots))
        overhead = statistics.get_overhead()
        expected_output = 2
        self.assertEqual(overhead, expected_output)

    def test_statistics_ma_single_1(self):
        agnostic_entity = AgnosticEntity('https://github.com/arcangelo7/time_agnostic/ar/15519', config=CONFIG)
        _, entity_snapshots, other_snapshots = agnostic_entity.get_state_at_time(time=('2021-06-01T18:46:41', '2021-06-01T18:46:41'), include_prov_metadata=True)
        statistics = Statistics((entity_snapshots, other_snapshots))
        overhead = statistics.get_overhead()
        expected_output = 1
        self.assertEqual(overhead, expected_output)

    def test_statistics_ma_single_interval(self):
        agnostic_entity = AgnosticEntity('https://github.com/arcangelo7/time_agnostic/ar/15519', config=CONFIG)
        _, entity_snapshots, other_snapshots = agnostic_entity.get_state_at_time(time=('2021-05-31T18:19:47', '2021-06-01T18:46:41'), include_prov_metadata=True)
        statistics = Statistics((entity_snapshots, other_snapshots))
        overhead = statistics.get_overhead()
        expected_output = 2
        self.assertEqual(overhead, expected_output)

    def test_statistics_cd(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime. 
            }
        """
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}   
        delta_query = DeltaQuery(query=query, changed_properties=changed_properties, config_path=CONFIG_PATH)
        delta_query.run_agnostic_query()
        statistics = Statistics(delta_query.reconstructed_entities)
        overhead = statistics.get_overhead()
        expected_output = 1
        self.assertEqual(overhead, expected_output)

    def test_statistics_sd(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime. 
            }
        """
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}   
        delta_query = DeltaQuery(query=query, on_time=("2021-06-02T18:46:41", "2021-06-02T18:46:41"), changed_properties=changed_properties, config_path=CONFIG_PATH)
        delta_query.run_agnostic_query()
        statistics = Statistics(delta_query.reconstructed_entities)
        overhead = statistics.get_overhead()
        expected_output = 1
        self.assertEqual(overhead, expected_output)


