# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import json
import os
import subprocess
from datetime import datetime

import pytest
from qlever_fixture import BASE, END, OBJECT, PREDICATE, START, fixture_provenance
from rdflib import Literal, URIRef
from triplestore_config import CONFIG, CONFIG_PROV_IN_TRIPLESTORE

from time_agnostic_library.agnostic_query import (
    DeltaQuery,
    VersionQuery,
    _batch_query_provenance_snapshots,
)
from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.qlever import QLEVER_HAS_WORD, generate_qlever_index
from time_agnostic_library.qlever_setup import setup_qlever
from time_agnostic_library.sparql import Sparql
from time_agnostic_library.support import generate_config_file


def test_generate_index_preserves_rdf_uri_terms():
    update = (
        f"INSERT DATA {{ GRAPH <{BASE}graph> {{ <{BASE}S> <{PREDICATE}> "
        f'<{OBJECT}> . <{BASE}S> <{PREDICATE}> "{BASE}literal" . '
        f'<{BASE}S> <{PREDICATE}> "x"^^<{BASE}datatype> . }} }}; '
        f"DELETE DATA {{ GRAPH <{BASE}graph> {{ <{BASE}S> <{PREDICATE}> "
        f"<{OBJECT}> . }} }}"
    )
    assert generate_qlever_index(
        [
            (BASE + "snapshot1", update),
            (BASE + "snapshot1", update),
            (BASE + "snapshot2", update),
        ]
    ) == {
        (URIRef(BASE + snapshot), QLEVER_HAS_WORD, Literal(term))
        for snapshot in ("snapshot1", "snapshot2")
        for term in (BASE + "S", PREDICATE, OBJECT)
    }


def test_generate_qlever_config(tmp_path):
    path = tmp_path / "config.json"
    config = generate_config_file(str(path), qlever_full_text_search=True)
    assert json.loads(path.read_text()) == config
    query = VersionQuery(
        "SELECT ?s WHERE { ?s <https://example.org/p> ?o }",
        config_dict=config,
    )
    assert query.qlever_full_text_search is True


@pytest.mark.parametrize(
    "adapter",
    [
        "blazegraph_full_text_search",
        "fuseki_full_text_search",
        "virtuoso_full_text_search",
        "graphdb_connector_name",
    ],
)
def test_qlever_rejects_other_adapters(adapter):
    config = {**CONFIG, "qlever_full_text_search": "yes", adapter: "yes"}
    with pytest.raises(ValueError, match="multiple indexing systems"):
        VersionQuery("SELECT ?s WHERE { ?s ?p ?o }", config_dict=config)


@pytest.mark.parametrize(
    ("terms", "names"),
    [
        ({PREDICATE, OBJECT}, ["match", "split", "wrong-position"]),
        ({BASE + "match", PREDICATE, OBJECT}, ["match"]),
    ],
)
def test_remote_index_selects_whole_uri_candidates(terms, names):
    config = {**CONFIG_PROV_IN_TRIPLESTORE, "qlever_full_text_search": "yes"}
    query = VersionQuery(
        f"SELECT ?s WHERE {{ ?s <{PREDICATE}> <{OBJECT}> }}", config_dict=config
    )
    candidates = Sparql(query.get_full_text_search(terms), config).run_select_query()
    provenance = fixture_provenance()
    expected = [
        str(update)
        for name in names
        for number in (1, 2)
        for update in provenance.objects(
            URIRef(f"{BASE}{name}/prov/se/{number}"),
            URIRef(ProvEntity.iri_has_update_query),
        )
    ]
    assert sorted(
        binding["updateQuery"]["value"] for binding in candidates["results"]["bindings"]
    ) == sorted(expected)


@pytest.mark.parametrize(
    ("predicate", "object_", "names"),
    [
        (PREDICATE, OBJECT, ["match"]),
        (BASE + "predicate", OBJECT, ["lower"]),
        (PREDICATE, OBJECT + "/child", ["namespace"]),
        (PREDICATE, BASE, []),
        (PREDICATE, BASE + "missing", []),
        (PREDICATE, BASE + "école東京", []),
        (PREDICATE, BASE + "literal-only", []),
        (OBJECT, PREDICATE, ["wrong-position"]),
    ],
)
@pytest.mark.parametrize("indexed", [False, True])
def test_remote_queries_match_exact_history(predicate, object_, names, indexed):
    config = {
        **CONFIG_PROV_IN_TRIPLESTORE,
        "qlever_full_text_search": "yes" if indexed else "no",
    }
    query = f"SELECT ?s WHERE {{ ?s <{predicate}> <{object_}> }}"
    bindings = [{"s": {"type": "uri", "value": BASE + name}} for name in names]
    expected_version = {START: bindings, END: []} if names else {}
    assert VersionQuery(query, config_dict=config).run_agnostic_query() == (
        expected_version,
        None,
        None,
    )
    expected_delta = {
        "merges": None,
        "additions": [],
        "deletions": bindings,
        "changes": [
            {"start": START, "end": END, "additions": [], "deletions": bindings}
        ]
        if names
        else [],
    }
    assert DeltaQuery(query, config_dict=config).run_agnostic_query() == (
        expected_delta,
        None,
        None,
    )


@pytest.mark.parametrize("indexed", [False, True])
def test_remote_interval_retains_snapshot_before_threshold(indexed):
    config = {
        **CONFIG_PROV_IN_TRIPLESTORE,
        "qlever_full_text_search": "yes" if indexed else "no",
    }
    middle = "2024-01-15T00:00:00+00:00"
    query = f"SELECT ?s WHERE {{ ?s <{PREDICATE}> <{OBJECT}> }}"
    binding = {"s": {"type": "uri", "value": BASE + "match"}}
    assert VersionQuery(
        query, on_time=(middle, END), config_dict=config
    ).run_agnostic_query() == (
        {middle: [binding], END: []},
        None,
        None,
    )
    assert DeltaQuery(
        query, on_time=(middle, END), config_dict=config
    ).run_agnostic_query() == (
        {
            "merges": None,
            "additions": [],
            "deletions": [binding],
            "changes": [
                {"start": middle, "end": END, "additions": [], "deletions": [binding]},
            ],
        },
        None,
        None,
    )
    data = fixture_provenance()
    update = str(
        data.value(
            URIRef(BASE + "match/prov/se/2"), URIRef(ProvEntity.iri_has_update_query)
        )
    )
    result = _batch_query_provenance_snapshots({BASE + "match"}, config, middle)
    for rows in result.values():
        for row in rows:
            row["time"] = datetime.fromisoformat(
                row["time"].replace("Z", "+00:00")
            ).isoformat()
    assert {
        entity: sorted(rows, key=lambda row: row["time"])
        for entity, rows in result.items()
    } == {
        BASE + "match": [
            {"time": START, "updateQuery": None},
            {"time": END, "updateQuery": update},
        ],
    }


@pytest.mark.parametrize("local", [False, True])
@pytest.mark.parametrize("has_uri", [False, True])
def test_qlever_fallbacks(tmp_path, local, has_uri):
    config = {**CONFIG_PROV_IN_TRIPLESTORE, "qlever_full_text_search": "yes"}
    if local:
        path = tmp_path / "prov.json"
        fixture_provenance().serialize(path, format="json-ld")
        config["provenance"] = {
            "triplestore_urls": [],
            "file_paths": [str(path)],
            "is_quadstore": True,
        }
    pattern = f"<{PREDICATE}> <{OBJECT}>" if has_uri else f'?p "{BASE}literal-only"'
    name = "match" if has_uri else "literal-only"
    projection = "?s" if has_uri else "?s ?p"
    query = f"SELECT {projection} WHERE {{ ?s {pattern} }}"
    binding = {"s": {"type": "uri", "value": BASE + name}}
    if not has_uri:
        binding["p"] = {"type": "uri", "value": PREDICATE}
    expected = (
        {START: [binding], END: []},
        None,
        None,
    )
    assert VersionQuery(query, config_dict=config).run_agnostic_query() == expected


@pytest.mark.skipif(
    os.environ.get("TRIPLESTORE") != "qlever", reason="Requires QLever Docker"
)
def test_setup_qlever_serves_history(tmp_path):
    dataset = tmp_path / "dataset.nq"
    dataset.write_text(
        '<urn:dataset:s> <urn:dataset:p> "value" <urn:dataset:g> .\n',
        encoding="utf-8",
    )
    provenance = tmp_path / "provenance.nq"
    fixture_provenance().serialize(provenance, format="nquads")
    try:
        config = setup_qlever([dataset], [provenance], tmp_path / "index", port=41761)
        query = f"SELECT ?s WHERE {{ ?s <{PREDICATE}> <{OBJECT}> }}"
        binding = {"s": {"type": "uri", "value": BASE + "match"}}
        assert VersionQuery(query, config_path=str(config)).run_agnostic_query() == (
            {START: [binding], END: []},
            None,
            None,
        )
        assert DeltaQuery(query, config_path=str(config)).run_agnostic_query() == (
            {
                "merges": None,
                "additions": [],
                "deletions": [binding],
                "changes": [
                    {
                        "start": START,
                        "end": END,
                        "additions": [],
                        "deletions": [binding],
                    }
                ],
            },
            None,
            None,
        )
        settings = json.loads(config.read_text())
        assert Sparql(
            "SELECT ?o WHERE { GRAPH <urn:dataset:g> { "
            "<urn:dataset:s> <urn:dataset:p> ?o } }",
            settings,
        ).run_select_query()["results"]["bindings"] == [
            {"o": {"type": "literal", "value": "value"}}
        ]
        assert settings["qlever_full_text_search"] == "true"
        assert settings["provenance"] == {
            "triplestore_urls": ["http://localhost:41761"],
            "file_paths": [],
            "is_quadstore": True,
        }
    finally:
        subprocess.run(["docker", "rm", "-f", "tal-qlever-41761"], check=True)  # noqa: S607
