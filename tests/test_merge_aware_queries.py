# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from unittest.mock import patch

from triplestore_config import CONFIG_PROV_IN_TRIPLESTORE

from time_agnostic_library.agnostic_query import (
    AgnosticQuery,
    DeltaQuery,
    VersionQuery,
)
from time_agnostic_library.support import convert_to_datetime

_BASE = "https://example.org/merge/"
_GRAPH = f"<{_BASE}>"
_LABEL = f"<{_BASE}label>"
_RELATED_TO = f"<{_BASE}relatedTo>"
_T1 = "2021-01-01T00:00:00+00:00"
_T2 = "2021-02-01T00:00:00+00:00"
_T3 = "2021-03-01T00:00:00+00:00"


def _uri(name):
    return f"{_BASE}{name}"


def _binding(entity, label):
    return {
        "entity": {"type": "uri", "value": _uri(entity)},
        "label": {"type": "literal", "value": label},
    }


def _sorted_bindings(results):
    return {
        timestamp: sorted(bindings, key=lambda binding: binding["entity"]["value"])
        for timestamp, bindings in results.items()
    }


def _merge_event(time, snapshot, survivor, absorbed):
    return {
        "time": time,
        "snapshot": f"{_uri(snapshot)}/prov/se/2",
        "survivor": _uri(survivor),
        "absorbed": [_uri(entity) for entity in absorbed],
    }


def _sorted_merge_events(events):
    return sorted(
        ({**event, "absorbed": sorted(event["absorbed"])} for event in events),
        key=lambda event: (event["time"], event["snapshot"]),
    )


def _deletions(entity, label):
    entity_n3 = f"<{_uri(entity)}>"
    return {
        (entity_n3, _LABEL, f'"{label}"', _GRAPH),
        (entity_n3, _RELATED_TO, entity_n3, _GRAPH),
    }


def _delete_query(entity, label):
    entity_iri = _uri(entity)
    return (
        f"DELETE DATA {{ GRAPH <{_BASE}> {{ <{entity_iri}> <{_BASE}label> "
        f'"{label}" . <{entity_iri}> <{_BASE}relatedTo> <{entity_iri}> . }} }}'
    )


def _normalized_metadata(metadata):
    return {
        entity: {
            snapshot: {
                key: str(convert_to_datetime(value, stringify=True))
                if value is not None and key in {"generatedAtTime", "invalidatedAtTime"}
                else value
                for key, value in fields.items()
            }
            for snapshot, fields in snapshots.items()
        }
        for entity, snapshots in metadata.items()
    }


def test_merge_aware_version_query_keeps_historical_iris():
    query = f"""
        SELECT ?entity ?label WHERE {{
            ?entity <{_BASE}label> ?label.
        }}
    """
    results, provenance, other_provenance = VersionQuery(
        query,
        merge_aware=True,
        config_dict=CONFIG_PROV_IN_TRIPLESTORE,
    ).run_agnostic_query()
    expected = {
        _T1: [
            _binding("a", "A"),
            _binding("b", "B"),
            _binding("c", "C"),
            _binding("d", "D"),
        ],
        _T2: [_binding("c", "C"), _binding("d", "D")],
        _T3: [_binding("d", "D")],
    }
    assert _sorted_bindings(results) == _sorted_bindings(expected)
    assert provenance is None
    assert other_provenance is None


def test_discovered_variables_follow_merge_histories():
    query = f"""
        SELECT ?entity ?label WHERE {{
            <{_uri("a")}> <{_BASE}relatedTo> ?entity.
            ?entity <{_BASE}label> ?label.
        }}
    """
    results, _, _ = VersionQuery(
        query,
        merge_aware=True,
        config_dict=CONFIG_PROV_IN_TRIPLESTORE,
    ).run_agnostic_query()
    expected = {
        _T1: [
            _binding("a", "A"),
            _binding("b", "B"),
            _binding("c", "C"),
            _binding("d", "D"),
        ],
        _T2: [_binding("c", "C"), _binding("d", "D")],
        _T3: [_binding("d", "D")],
    }
    assert _sorted_bindings(results) == _sorted_bindings(expected)


def test_each_merged_iri_matches_subject_and_object_constants():
    expected = {
        _T1: [_uri("a"), _uri("b"), _uri("c"), _uri("d")],
        _T2: [_uri("c"), _uri("d")],
        _T3: [_uri("d")],
    }
    for entity in ("a", "b", "c", "d"):
        object_query = f"""
            SELECT ?actual WHERE {{
                ?actual <{_BASE}relatedTo> <{_uri(entity)}>.
            }}
        """
        results, _, _ = VersionQuery(
            object_query,
            merge_aware=True,
            config_dict=CONFIG_PROV_IN_TRIPLESTORE,
        ).run_agnostic_query()
        actual = {
            timestamp: sorted(binding["actual"]["value"] for binding in bindings)
            for timestamp, bindings in results.items()
        }
        assert actual == expected

        subject_query = f"""
            SELECT ?label WHERE {{
                <{_uri(entity)}> <{_BASE}label> ?label.
            }}
        """
        subject_results, _, _ = VersionQuery(
            subject_query,
            merge_aware=True,
            config_dict=CONFIG_PROV_IN_TRIPLESTORE,
        ).run_agnostic_query()
        actual_labels = {
            timestamp: sorted(binding["label"]["value"] for binding in bindings)
            for timestamp, bindings in subject_results.items()
        }
        assert actual_labels == {
            _T1: ["A", "B", "C", "D"],
            _T2: ["C", "D"],
            _T3: ["D"],
        }


def test_provenance_channels_are_independent_from_merge_support():
    query = f"SELECT ?label WHERE {{ <{_uri('c')}> <{_BASE}label> ?label. }}"
    results, provenance, other_provenance = VersionQuery(
        query,
        on_time=(_T2, _T2),
        include_prov_metadata=True,
        config_dict=CONFIG_PROV_IN_TRIPLESTORE,
    ).run_agnostic_query()
    assert results == {
        _T2: [{"label": {"type": "literal", "value": "C"}}],
    }
    assert provenance is not None
    assert _normalized_metadata(provenance) == {
        _uri("c"): {
            f"{_uri('c')}/prov/se/2": {
                "generatedAtTime": _T2,
                "invalidatedAtTime": _T3,
                "wasAttributedTo": "https://example.org/agent",
                "hasUpdateQuery": None,
                "hadPrimarySource": None,
                "description": "Entities a and b were merged into c.",
                "wasDerivedFrom": [
                    f"{_uri('a')}/prov/se/1",
                    f"{_uri('b')}/prov/se/1",
                    f"{_uri('c')}/prov/se/1",
                ],
            }
        }
    }
    assert other_provenance is not None
    assert _normalized_metadata(other_provenance) == {
        _uri("c"): {
            f"{_uri('c')}/prov/se/1": {
                "generatedAtTime": _T1,
                "invalidatedAtTime": _T2,
                "wasAttributedTo": "https://example.org/agent",
                "hasUpdateQuery": None,
                "hadPrimarySource": None,
                "description": "Entity c was created.",
                "wasDerivedFrom": [],
            },
            f"{_uri('c')}/prov/se/3": {
                "generatedAtTime": _T3,
                "invalidatedAtTime": _T3,
                "wasAttributedTo": "https://example.org/agent",
                "hasUpdateQuery": _delete_query("c", "C"),
                "hadPrimarySource": None,
                "description": "Entity c was deleted.",
                "wasDerivedFrom": [f"{_uri('c')}/prov/se/2"],
            },
        }
    }


def test_point_version_query_combines_entities_with_staggered_snapshots():
    query = f"SELECT ?entity ?label WHERE {{ ?entity <{_BASE}label> ?label. }}"

    results, provenance, other_provenance = VersionQuery(
        query,
        on_time=(_T2, _T2),
        config_dict=CONFIG_PROV_IN_TRIPLESTORE,
    ).run_agnostic_query()

    assert list(results) == [_T2]
    assert sorted(results[_T2], key=lambda binding: binding["entity"]["value"]) == [
        {
            "entity": {"type": "uri", "value": _uri("c")},
            "label": {"type": "literal", "value": "C"},
        },
        {
            "entity": {"type": "uri", "value": _uri("d")},
            "label": {"type": "literal", "value": "D"},
        },
    ]
    assert provenance is None
    assert other_provenance is None

    empty_results, empty_provenance, empty_other_provenance = VersionQuery(
        "SELECT ?o WHERE { <https://example.org/missing> ?p ?o. }",
        include_prov_metadata=True,
        config_dict=CONFIG_PROV_IN_TRIPLESTORE,
    ).run_agnostic_query()
    assert empty_results == {}
    assert empty_provenance == {}
    assert empty_other_provenance == {}


def test_delta_merge_aware_compares_solution_mappings():
    query = f"SELECT ?label WHERE {{ <{_uri('a')}> <{_BASE}label> ?label. }}"
    result, provenance, other_provenance = DeltaQuery(
        query,
        on_time=(_T1, _T3),
        merge_aware=True,
        config_dict=CONFIG_PROV_IN_TRIPLESTORE,
    ).run_agnostic_query()

    def label(value):
        return {"label": {"type": "literal", "value": value}}

    def ordered(bindings):
        return sorted(bindings, key=lambda binding: binding["label"]["value"])

    normalized_result = {
        **result,
        "additions": ordered(result["additions"]),
        "deletions": ordered(result["deletions"]),
        "merges": _sorted_merge_events(result["merges"]),
        "changes": [
            {
                **change,
                "additions": ordered(change["additions"]),
                "deletions": ordered(change["deletions"]),
            }
            for change in result["changes"]
        ],
    }
    assert normalized_result == {
        "additions": [],
        "deletions": [label("A"), label("B"), label("C")],
        "merges": [
            _merge_event(_T2, "c", "c", ["a", "b"]),
            _merge_event(_T3, "d", "d", ["c"]),
        ],
        "changes": [
            {
                "start": _T1,
                "end": _T2,
                "additions": [],
                "deletions": [label("A"), label("B")],
            },
            {
                "start": _T2,
                "end": _T3,
                "additions": [],
                "deletions": [label("C")],
            },
        ],
    }
    assert provenance is None
    assert other_provenance is None

    point_result, _, _ = DeltaQuery(
        query,
        on_time=(_T3, _T3),
        merge_aware=True,
        config_dict=CONFIG_PROV_IN_TRIPLESTORE,
    ).run_agnostic_query()
    assert {
        **point_result,
        "merges": _sorted_merge_events(point_result["merges"]),
    } == {
        "additions": [],
        "deletions": [],
        "changes": [],
        "merges": [
            _merge_event(_T2, "c", "c", ["a", "b"]),
            _merge_event(_T3, "d", "d", ["c"]),
        ],
    }


def test_disabled_merge_support_issues_no_merge_queries():
    query = f"SELECT ?label WHERE {{ <{_uri('d')}> <{_BASE}label> ?label. }}"
    with patch.object(AgnosticQuery, "_query_adjacent_merge_events") as merge_query:
        version_results, version_provenance, version_other_provenance = VersionQuery(
            query,
            config_dict=CONFIG_PROV_IN_TRIPLESTORE,
        ).run_agnostic_query()
        delta_results, delta_provenance, delta_other_provenance = DeltaQuery(
            query,
            config_dict=CONFIG_PROV_IN_TRIPLESTORE,
        ).run_agnostic_query()
    assert version_results == {
        _T1: [{"label": {"type": "literal", "value": "D"}}],
        _T3: [{"label": {"type": "literal", "value": "D"}}],
    }
    assert version_provenance is None
    assert version_other_provenance is None
    assert delta_results == {
        "additions": [],
        "deletions": [],
        "merges": None,
        "changes": [
            {
                "start": _T1,
                "end": _T3,
                "additions": [],
                "deletions": [],
            }
        ],
    }
    assert delta_provenance is None
    assert delta_other_provenance is None
    merge_query.assert_not_called()
