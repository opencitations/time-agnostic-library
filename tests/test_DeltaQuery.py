# SPDX-FileCopyrightText: 2021-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from triplestore_config import CONFIG, CONFIG_PROV_IN_TRIPLESTORE

from time_agnostic_library.agnostic_query import DeltaQuery, _build_delta_result

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
    (
        _RA_15519,
        "<http://xmlns.com/foaf/0.1/name>",
        '"Giulio Marini"^^<http://www.w3.org/2001/XMLSchema#string>',
        _RA_GRAPH,
    ),
    (
        _RA_15519,
        "<http://xmlns.com/foaf/0.1/givenName>",
        '"Giulio"^^<http://www.w3.org/2001/XMLSchema#string>',
        _RA_GRAPH,
    ),
    (
        _RA_15519,
        "<http://purl.org/spar/datacite/hasIdentifier>",
        f"<{_ID}/85509>",
        _RA_GRAPH,
    ),
    (
        _RA_15519,
        "<http://xmlns.com/foaf/0.1/familyName>",
        '"Marini"^^<http://www.w3.org/2001/XMLSchema#string>',
        _RA_GRAPH,
    ),
    (
        _RA_15519,
        "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
        "<http://xmlns.com/foaf/0.1/Agent>",
        _RA_GRAPH,
    ),
}

_UNRELATED_CHANGE_TIME = "2021-05-31T18:19:47+00:00"
_ISHELDBY_CHANGE_TIME = "2021-06-01T18:46:41+00:00"

_ISHELDBY_CHANGE = {
    "time": _ISHELDBY_CHANGE_TIME,
    "additions": {_ISHELDBY_ADDITION},
    "deletions": {_ISHELDBY_DELETION},
}
_UNRELATED_CHANGE = {
    "time": _UNRELATED_CHANGE_TIME,
    "additions": set(),
    "deletions": set(),
}


class TestDeltaQuery:
    def test_run_agnostic_query_full_history(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime.
            }
        """
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}
        delta_query = DeltaQuery(
            query=query, changed_properties=changed_properties, config_dict=CONFIG
        )
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            f"{_AR}/15519": {
                "created": "2021-05-07T09:59:15+00:00",
                "deleted": None,
                "changes": [_UNRELATED_CHANGE, _ISHELDBY_CHANGE],
                "additions": {_ISHELDBY_ADDITION},
                "deletions": {_ISHELDBY_DELETION},
            }
        }
        assert agnostic_results == expected_output

    def test_run_agnostic_query_before(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime.
            }
        """
        on_time = (None, "2021-06-02T18:46:41+00:00")
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}
        delta_query = DeltaQuery(
            query=query,
            on_time=on_time,
            changed_properties=changed_properties,
            config_dict=CONFIG,
        )
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            f"{_AR}/15519": {
                "created": "2021-05-07T09:59:15+00:00",
                "deleted": None,
                "changes": [_UNRELATED_CHANGE, _ISHELDBY_CHANGE],
                "additions": {_ISHELDBY_ADDITION},
                "deletions": {_ISHELDBY_DELETION},
            }
        }
        assert agnostic_results == expected_output

    def test_run_agnostic_query_interval(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime.
            }
        """
        on_time = ("2021-06-01T00:00:00+00:00", "2021-06-02T18:46:41+00:00")
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}
        delta_query = DeltaQuery(
            query=query,
            on_time=on_time,
            changed_properties=changed_properties,
            config_dict=CONFIG,
        )
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            f"{_AR}/15519": {
                "created": None,
                "deleted": None,
                "changes": [_ISHELDBY_CHANGE],
                "additions": {_ISHELDBY_ADDITION},
                "deletions": {_ISHELDBY_DELETION},
            }
        }
        assert agnostic_results == expected_output

    def test_run_agnostic_query_after_history_returns_no_results(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime.
            }
        """
        on_time = ("2021-06-02T00:00:00+00:00", None)
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}
        delta_query = DeltaQuery(
            query=query,
            on_time=on_time,
            changed_properties=changed_properties,
            config_dict=CONFIG,
        )
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {}
        assert agnostic_results == expected_output

    def test_run_agnostic_query_on_deleted_entity(self):
        query = """
            prefix foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?ra
            WHERE {
                ?ra a foaf:Agent.
            }
        """
        delta_query = DeltaQuery(query=query, config_dict=CONFIG_PROV_IN_TRIPLESTORE)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            f"{_RA}/15519": {
                "created": "2021-05-07T09:59:15+00:00",
                "deleted": "2021-06-01T18:46:41+00:00",
                "changes": [
                    {
                        "time": _ISHELDBY_CHANGE_TIME,
                        "additions": set(),
                        "deletions": _DELETED_ENTITY_DELETIONS,
                    }
                ],
                "additions": set(),
                "deletions": _DELETED_ENTITY_DELETIONS,
            },
            f"{_RA}/4": {
                "created": "2021-05-07T09:59:15+00:00",
                "deleted": None,
                "changes": [],
                "additions": set(),
                "deletions": set(),
            },
        }
        assert agnostic_results == expected_output

    def test_run_agnostic_query_with_concrete_subject(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT ?p ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> ?p ?o .
            }
        """
        delta_query = DeltaQuery(query=query, config_dict=CONFIG)
        result = delta_query.run_agnostic_query()
        entity_key = f"{_AR}/15519"
        assert entity_key in result
        assert "created" in result[entity_key]
        assert "additions" in result[entity_key]
        assert "deletions" in result[entity_key]

    def test_run_agnostic_query_with_non_isolated_triples(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            SELECT ?ar ?agent
            WHERE {
                ?ar a pro:RoleInTime .
                ?ar pro:isHeldBy ?agent .
            }
        """
        delta_query = DeltaQuery(query=query, config_dict=CONFIG)
        result = delta_query.run_agnostic_query()
        assert isinstance(result, dict)
        assert len(result) > 0
        for entity_data in result.values():
            assert "additions" in entity_data
            assert "deletions" in entity_data

    def test_changes_are_chronological_and_compose_into_the_net_delta(self):
        quad = (
            "<http://example.org/s>",
            "<http://example.org/p>",
            "<http://example.org/o>",
            "<http://example.org/g/>",
        )
        insert = """
            INSERT DATA { GRAPH <http://example.org/g/> {
                <http://example.org/s> <http://example.org/p> <http://example.org/o> .
            } }
        """
        snapshots = [
            {
                "time": "2021-01-01T00:00:00+00:00",
                "updateQuery": None,
                "invalidatedAtTime": None,
            },
            {
                "time": "2021-02-01T00:00:00+00:00",
                "updateQuery": insert,
                "invalidatedAtTime": None,
            },
            {
                "time": "2021-03-01T00:00:00+00:00",
                "updateQuery": insert.replace("INSERT DATA", "DELETE DATA"),
                "invalidatedAtTime": None,
            },
        ]
        result = _build_delta_result(
            "http://example.org/entity", snapshots, None, set()
        )
        assert result == {
            "http://example.org/entity": {
                "created": "2021-01-01T00:00:00+00:00",
                "deleted": None,
                "changes": [
                    {
                        "time": "2021-02-01T00:00:00+00:00",
                        "additions": {quad},
                        "deletions": set(),
                    },
                    {
                        "time": "2021-03-01T00:00:00+00:00",
                        "additions": set(),
                        "deletions": {quad},
                    },
                ],
                "additions": set(),
                "deletions": set(),
            }
        }

    def test_changes_hold_the_delta_of_a_single_snapshot(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime.
            }
        """
        on_time = (_ISHELDBY_CHANGE_TIME, _ISHELDBY_CHANGE_TIME)
        delta_query = DeltaQuery(query=query, on_time=on_time, config_dict=CONFIG)
        result = delta_query.run_agnostic_query()
        record = result[f"{_AR}/15519"]
        assert len(record["changes"]) == 1
        change = record["changes"][0]
        assert change["time"] == _ISHELDBY_CHANGE_TIME
        assert (change["additions"], change["deletions"]) == (
            record["additions"],
            record["deletions"],
        )

    def test_run_agnostic_query_nonexistent_entity(self):
        query = """
            SELECT ?o WHERE {
                <https://github.com/arcangelo7/time_agnostic/nonexistent/999999> ?p ?o .
            }
        """
        delta_query = DeltaQuery(query=query, config_dict=CONFIG)
        result = delta_query.run_agnostic_query()
        assert result == {}

    def test_run_agnostic_query_nonexistent_type(self):
        query = """
            SELECT ?o WHERE {
                ?o a <http://example.com/NonExistentType> .
            }
        """
        delta_query = DeltaQuery(query=query, config_dict=CONFIG)
        result = delta_query.run_agnostic_query()
        assert result == {}
