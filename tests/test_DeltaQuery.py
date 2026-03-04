#!/usr/bin/python
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

from time_agnostic_library.agnostic_query import DeltaQuery

CONFIG_PATH = "tests/config.json"
CONFIG_VIRTUOSO = "tests/config_virtuoso.json"

_AR = "https://github.com/arcangelo7/time_agnostic/ar"
_RA = "https://github.com/arcangelo7/time_agnostic/ra"
_ID = "https://github.com/arcangelo7/time_agnostic/id"
_HELD_BY = "<http://purl.org/spar/pro/isHeldBy>"
_AR_GRAPH = f"<{_AR}/>"

_ISHELDBY_DELETION = (
    f"<{_AR}/15519>",
    _HELD_BY,
    f"<{_RA}/15519>",
    _AR_GRAPH,
)
_ISHELDBY_ADDITION = (
    f"<{_AR}/15519>",
    _HELD_BY,
    f"<{_RA}/4>",
    _AR_GRAPH,
)

_RA_GRAPH = f"<{_RA}/>"
_RA_15519 = f"<{_RA}/15519>"
_DELETED_ENTITY_DELETIONS = {
    (_RA_15519, '<http://xmlns.com/foaf/0.1/name>', '"Giulio Marini"^^<http://www.w3.org/2001/XMLSchema#string>', _RA_GRAPH),
    (_RA_15519, '<http://xmlns.com/foaf/0.1/givenName>', '"Giulio"^^<http://www.w3.org/2001/XMLSchema#string>', _RA_GRAPH),
    (_RA_15519, '<http://purl.org/spar/datacite/hasIdentifier>', f'<{_ID}/85509>', _RA_GRAPH),
    (_RA_15519, '<http://xmlns.com/foaf/0.1/familyName>', '"Marini"^^<http://www.w3.org/2001/XMLSchema#string>', _RA_GRAPH),
    (_RA_15519, '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>', '<http://xmlns.com/foaf/0.1/Agent>', _RA_GRAPH),
}


class Test_DeltaQuery(unittest.TestCase):
    def test_run_agnostic_query_cross_delta(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime.
            }
        """
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}
        delta_query = DeltaQuery(query=query, changed_properties=changed_properties, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            f'{_AR}/15519': {
                "created": "2021-05-07T09:59:15+00:00",
                "deleted": None,
                "additions": {_ISHELDBY_ADDITION},
                "deletions": {_ISHELDBY_DELETION},
            }
        }
        self.assertEqual(agnostic_results, expected_output)

    def test_run_agnostic_query_single_delta_before(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime.
            }
        """
        on_time = (None, "2021-06-02T18:46:41+00:00")
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}
        delta_query = DeltaQuery(query=query, on_time=on_time, changed_properties=changed_properties, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            f'{_AR}/15519': {
                "created": "2021-05-07T09:59:15+00:00",
                "deleted": None,
                "additions": {_ISHELDBY_ADDITION},
                "deletions": {_ISHELDBY_DELETION},
            }
        }
        self.assertEqual(agnostic_results, expected_output)

    def test_run_agnostic_query_single_delta_interval(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime.
            }
        """
        on_time = ("2021-06-01T00:00:00+00:00", "2021-06-02T18:46:41+00:00")
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}
        delta_query = DeltaQuery(query=query, on_time=on_time, changed_properties=changed_properties, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            f'{_AR}/15519': {
                "created": None,
                "deleted": None,
                "additions": {_ISHELDBY_ADDITION},
                "deletions": {_ISHELDBY_DELETION},
            }
        }
        self.assertEqual(agnostic_results, expected_output)

    def test_run_agnostic_query_single_delta_after_no_results(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime.
            }
        """
        on_time = ("2021-06-02T00:00:00+00:00", None)
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}
        delta_query = DeltaQuery(query=query, on_time=on_time, changed_properties=changed_properties, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {}
        self.assertEqual(agnostic_results, expected_output)

    def test_run_agnostic_query_on_deleted_entity(self):
        query = """
            prefix foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?ra
            WHERE {
                ?ra a foaf:Agent.
            }
        """
        delta_query = DeltaQuery(query=query, config_path=CONFIG_VIRTUOSO)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            f'{_RA}/15519': {
                'created': '2021-05-07T09:59:15+00:00',
                'deleted': '2021-06-01T18:46:41+00:00',
                'additions': set(),
                'deletions': _DELETED_ENTITY_DELETIONS,
            },
            f'{_RA}/4': {
                'created': '2021-05-07T09:59:15+00:00',
                'deleted': None,
                'additions': set(),
                'deletions': set(),
            }
        }
        self.assertEqual(agnostic_results, expected_output)

    def test_run_agnostic_query_with_concrete_subject(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT ?p ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> ?p ?o .
            }
        """
        delta_query = DeltaQuery(query=query, config_path=CONFIG_PATH)
        result = delta_query.run_agnostic_query()
        entity_key = f"{_AR}/15519"
        self.assertIn(entity_key, result)
        self.assertIn("created", result[entity_key])
        self.assertIn("additions", result[entity_key])
        self.assertIn("deletions", result[entity_key])

    def test_run_agnostic_query_with_non_isolated_triples(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT ?ar ?agent
            WHERE {
                ?ar a pro:RoleInTime .
                ?ar pro:isHeldBy ?agent .
            }
        """
        delta_query = DeltaQuery(query=query, config_path=CONFIG_PATH)
        result = delta_query.run_agnostic_query()
        self.assertIsInstance(result, dict)
        self.assertGreater(len(result), 0)
        for entity_data in result.values():
            self.assertIn("additions", entity_data)
            self.assertIn("deletions", entity_data)

    def test_run_agnostic_query_nonexistent_entity(self):
        query = """
            SELECT ?o WHERE {
                <https://github.com/arcangelo7/time_agnostic/nonexistent/999999> ?p ?o .
            }
        """
        delta_query = DeltaQuery(query=query, config_path=CONFIG_PATH)
        result = delta_query.run_agnostic_query()
        self.assertEqual(result, {})

    def test_run_agnostic_query_nonexistent_type(self):
        query = """
            SELECT ?o WHERE {
                ?o a <http://example.com/NonExistentType> .
            }
        """
        delta_query = DeltaQuery(query=query, config_path=CONFIG_PATH)
        result = delta_query.run_agnostic_query()
        self.assertEqual(result, {})


if __name__ == '__main__': # pragma: no cover
    unittest.main()
