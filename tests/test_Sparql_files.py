# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import json
import os
from pathlib import Path

import pytest

from time_agnostic_library.agnostic_entity import AgnosticEntity
from time_agnostic_library.sparql import Sparql

PERSON_QUERY = """
    SELECT ?name WHERE { ?person <http://xmlns.com/foaf/0.1/name> ?name . }
"""


def _config(dataset_files: list[str]) -> dict:
    return {
        "dataset": {
            "triplestore_urls": [],
            "file_paths": dataset_files,
            "is_quadstore": False,
        },
        "provenance": {
            "triplestore_urls": [],
            "file_paths": [],
            "is_quadstore": True,
        },
    }


def _names(path: Path) -> set[str]:
    results = Sparql(PERSON_QUERY, _config([str(path)])).run_select_query()
    return {binding["name"]["value"] for binding in results["results"]["bindings"]}


def test_select_from_turtle(tmp_path: Path):
    path = tmp_path / "data.ttl"
    path.write_text('<https://example.org/p1> <http://xmlns.com/foaf/0.1/name> "Ada" .')
    assert _names(path) == {"Ada"}


def test_select_from_trig_named_graphs(tmp_path: Path):
    path = tmp_path / "data.trig"
    path.write_text(
        "<https://example.org/g/> {"
        ' <https://example.org/p1> <http://xmlns.com/foaf/0.1/name> "Ada" . }'
    )
    assert _names(path) == {"Ada"}


def test_rewritten_file_is_read_again(tmp_path: Path):
    path = tmp_path / "data.ttl"
    path.write_text('<https://example.org/p1> <http://xmlns.com/foaf/0.1/name> "Ada" .')
    assert _names(path) == {"Ada"}
    path.write_text(
        '<https://example.org/p1> <http://xmlns.com/foaf/0.1/name> "Grace Hopper" .'
    )
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
    assert _names(path) == {"Grace Hopper"}


def test_ask_on_files(tmp_path: Path):
    path = tmp_path / "data.ttl"
    path.write_text('<https://example.org/p1> <http://xmlns.com/foaf/0.1/name> "Ada" .')
    config = _config([str(path)])
    assert Sparql(
        'ASK { ?s <http://xmlns.com/foaf/0.1/name> "Ada" }', config
    ).run_ask_query()
    assert not Sparql(
        'ASK { ?s <http://xmlns.com/foaf/0.1/name> "Bob" }', config
    ).run_ask_query()


def test_unsupported_extension(tmp_path: Path):
    path = tmp_path / "data.xml"
    path.write_text("")
    with pytest.raises(ValueError, match="Unsupported file extension"):
        _names(path)


def test_history_of_a_triple_dataset_in_files(tmp_path: Path):
    entity = "https://example.org/br/1"
    data = tmp_path / "data.ttl"
    data.write_text(f'<{entity}> <http://purl.org/dc/terms/title> "Changed" .')
    provenance = tmp_path / "provenance.trig"
    prov = "http://www.w3.org/ns/prov#"
    xsd = "http://www.w3.org/2001/XMLSchema#"
    update = (
        f'DELETE DATA {{ <{entity}> <http://purl.org/dc/terms/title> "First" . }}; '
        f'INSERT DATA {{ <{entity}> <http://purl.org/dc/terms/title> "Changed" . }}'
    )
    provenance.write_text(f"""
        <{entity}/prov/> {{
            <{entity}/prov/se/1> a <{prov}Entity> ;
                <{prov}specializationOf> <{entity}> ;
                <{prov}generatedAtTime> "2026-01-01T00:00:00+00:00"^^<{xsd}dateTime> ;
                <{prov}invalidatedAtTime> "2026-02-01T00:00:00+00:00"^^<{xsd}dateTime> ;
                <{prov}wasAttributedTo> <https://orcid.org/0000-0002-8420-0696> ;
                <http://purl.org/dc/terms/description> "Created." .
            <{entity}/prov/se/2> a <{prov}Entity> ;
                <{prov}specializationOf> <{entity}> ;
                <{prov}wasDerivedFrom> <{entity}/prov/se/1> ;
                <{prov}generatedAtTime> "2026-02-01T00:00:00+00:00"^^<{xsd}dateTime> ;
                <{prov}wasAttributedTo> <https://orcid.org/0000-0002-8420-0696> ;
                <http://purl.org/dc/terms/description> "Modified." ;
                <https://w3id.org/oc/ontology/hasUpdateQuery> {json.dumps(update)} .
        }}
    """)
    config = _config([str(data)])
    config["provenance"]["file_paths"] = [str(provenance)]

    history, _ = AgnosticEntity(entity, config=config).get_history()

    title = "<http://purl.org/dc/terms/title>"
    assert history[entity] == {
        "2026-01-01T00:00:00+00:00": {(f"<{entity}>", title, '"First"')},
        "2026-02-01T00:00:00+00:00": {(f"<{entity}>", title, '"Changed"')},
    }
