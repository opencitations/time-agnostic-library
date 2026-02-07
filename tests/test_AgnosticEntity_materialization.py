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
    "graphdb_connector_name": ""
}

class TestAgnosticEntityMaterialization(unittest.TestCase):
    maxDiff = None

    def test_materialization_with_blank_node(self):
        agnostic_entity = AgnosticEntity('https://github.com/arcangelo7/time_agnostic/br/528728', config=CONFIG)
        output = _to_dict_of_nt_sorted_lists(agnostic_entity.get_history()[0])
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/br/528728': {
                '2021-09-13T16:42:27+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/br/528728> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/282404>', 
                    '<https://github.com/arcangelo7/time_agnostic/br/528728> <http://purl.org/spar/datacite/hasIdentifier> _:identifier', 
                    '<https://github.com/arcangelo7/time_agnostic/br/528728> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>', 
                    '<https://github.com/arcangelo7/time_agnostic/br/528728> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalArticle>'], 
                '2021-09-13T16:51:57+00:00': [
                    '<https://github.com/arcangelo7/time_agnostic/br/528728> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/282404>', 
                    '<https://github.com/arcangelo7/time_agnostic/br/528728> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>', 
                    '<https://github.com/arcangelo7/time_agnostic/br/528728> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalArticle>']}}
        self.assertEqual(output, expected_output)

    def test_materialization_sv_with_blank_node(self):
        agnostic_entity = AgnosticEntity('https://github.com/arcangelo7/time_agnostic/br/528728', config=CONFIG)
        output = _to_dict_of_nt_sorted_lists(agnostic_entity.get_state_at_time(time=('2021-09-13T16:42:27', '2021-09-13T16:42:27'))[0])
        expected_output = {'2021-09-13T16:42:27+00:00': [
            '<https://github.com/arcangelo7/time_agnostic/br/528728> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/282404>', 
            '<https://github.com/arcangelo7/time_agnostic/br/528728> <http://purl.org/spar/datacite/hasIdentifier> _:identifier', 
            '<https://github.com/arcangelo7/time_agnostic/br/528728> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/Expression>', 
            '<https://github.com/arcangelo7/time_agnostic/br/528728> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://purl.org/spar/fabio/JournalArticle>']}
        self.assertEqual(output, expected_output)


if __name__ == '__main__': # pragma: no cover
    unittest.main() 