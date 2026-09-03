# SPDX-FileCopyrightText: 2021-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from unittest.mock import patch

from triplestore_config import CONFIG

from time_agnostic_library.agnostic_query import (
    DeltaQuery,
    VersionQuery,
    _build_solution_delta,
)

_AR = "https://github.com/arcangelo7/time_agnostic/ar/15519"
_OLD_AGENT = "https://github.com/arcangelo7/time_agnostic/ra/15519"
_NEW_AGENT = "https://github.com/arcangelo7/time_agnostic/ra/4"
_START = "2021-05-07T09:59:15+00:00"
_UNRELATED_CHANGE = "2021-05-31T18:19:47+00:00"
_END = "2021-06-01T18:46:41+00:00"


def _uri_binding(variable: str, value: str) -> dict:
    return {variable: {"type": "uri", "value": value}}


def _run(query: str, on_time=None):
    result, provenance, other_provenance = DeltaQuery(
        query,
        on_time=on_time,
        config_dict=CONFIG,
    ).run_agnostic_query()
    assert provenance is None
    assert other_provenance is None
    return result


def test_solution_delta_preserves_duplicate_solution_mappings():
    a = _uri_binding("value", "https://example.org/a")
    b = _uri_binding("value", "https://example.org/b")
    results = {
        "2021-01-01T00:00:00+00:00": [a, a, b],
        "2021-01-02T00:00:00+00:00": [a, b, b],
    }

    assert _build_solution_delta(results, None) == {
        "additions": [b],
        "deletions": [a],
        "changes": [
            {
                "start": "2021-01-01T00:00:00+00:00",
                "end": "2021-01-02T00:00:00+00:00",
                "additions": [b],
                "deletions": [a],
            }
        ],
    }


def test_solution_delta_uses_interval_endpoints_and_consecutive_states():
    a = _uri_binding("value", "https://example.org/a")
    b = _uri_binding("value", "https://example.org/b")
    c = _uri_binding("value", "https://example.org/c")
    results = {
        "2021-01-01T00:00:00+00:00": [a],
        "2021-01-02T00:00:00+00:00": [b],
        "2021-01-03T00:00:00+00:00": [c],
    }

    assert _build_solution_delta(
        results,
        ("2021-01-01T12:00:00+00:00", "2021-01-02T12:00:00+00:00"),
    ) == {
        "additions": [b],
        "deletions": [a],
        "changes": [
            {
                "start": "2021-01-01T12:00:00+00:00",
                "end": "2021-01-02T00:00:00+00:00",
                "additions": [b],
                "deletions": [a],
            },
            {
                "start": "2021-01-02T00:00:00+00:00",
                "end": "2021-01-02T12:00:00+00:00",
                "additions": [],
                "deletions": [],
            },
        ],
    }


def test_delta_query_returns_solution_mapping_differences_for_a_bgp():
    query = f"""
        PREFIX pro: <http://purl.org/spar/pro/>
        SELECT ?agent WHERE {{
            <{_AR}> a pro:RoleInTime;
                pro:isHeldBy ?agent.
        }}
    """
    old_agent = _uri_binding("agent", _OLD_AGENT)
    new_agent = _uri_binding("agent", _NEW_AGENT)

    assert _run(query, (_START, _END)) == {
        "additions": [new_agent],
        "deletions": [],
        "merges": None,
        "changes": [
            {
                "start": _START,
                "end": _UNRELATED_CHANGE,
                "additions": [old_agent],
                "deletions": [],
            },
            {
                "start": _UNRELATED_CHANGE,
                "end": _END,
                "additions": [new_agent],
                "deletions": [old_agent],
            },
        ],
    }


def test_delta_query_runs_one_version_query_for_a_bgp():
    query = f"""
        PREFIX pro: <http://purl.org/spar/pro/>
        SELECT ?agent WHERE {{
            <{_AR}> a pro:RoleInTime;
                pro:isHeldBy ?agent.
        }}
    """

    with patch(
        "time_agnostic_library.agnostic_query.VersionQuery",
        wraps=VersionQuery,
    ) as version_query:
        _run(query, (_START, _END))

    assert version_query.call_count == 1


def test_delta_query_evaluates_optional_patterns_at_each_endpoint():
    query = f"""
        PREFIX pro: <http://purl.org/spar/pro/>
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT DISTINCT ?agent ?name WHERE {{
            <{_AR}> pro:isHeldBy ?agent.
            OPTIONAL {{ ?agent foaf:name ?name. }}
        }}
    """
    result = _run(query, (_START, _END))

    assert result["additions"] == [
        {
            "agent": {"type": "uri", "value": _NEW_AGENT},
            "name": {
                "type": "literal",
                "value": "Giulio Marini",
            },
        }
    ]
    assert result["deletions"] == [
        {
            "agent": {"type": "uri", "value": _OLD_AGENT},
            "name": {
                "type": "literal",
                "value": "Giulio Marini",
                "datatype": "http://www.w3.org/2001/XMLSchema#string",
            },
        }
    ]


def test_delta_query_returns_empty_bags_for_a_missing_pattern():
    result = _run("SELECT ?value WHERE { ?value a <https://example.org/MissingType>. }")

    assert result == {
        "additions": [],
        "deletions": [],
        "changes": [],
        "merges": None,
    }
