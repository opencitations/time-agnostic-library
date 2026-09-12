# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import shutil
import subprocess
import time
from pathlib import Path

import requests
from qlever_fixture import fixture_provenance
from rdflib import Dataset, URIRef
from sparqlite import SPARQLClient
from triplestore_config import ENDPOINT, TRIPLESTORE

from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.qlever import generate_qlever_index

TESTS_DIR = Path(__file__).parent
QLEVER_DIR = TESTS_DIR / "qlever_data"
UPDATE_ENDPOINT = ENDPOINT + "/statements" if TRIPLESTORE == "graphdb" else ENDPOINT

_BLAZEGRAPH_NAMESPACE = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
  <entry key="com.bigdata.rdf.sail.namespace">tal</entry>
  <entry key="com.bigdata.rdf.store.AbstractTripleStore.quads">true</entry>
  <entry key="com.bigdata.rdf.store.AbstractTripleStore.textIndex">true</entry>
  <entry key="com.bigdata.rdf.sail.truthMaintenance">false</entry>
  <entry key="com.bigdata.rdf.store.AbstractTripleStore.axiomsClass">com.bigdata.rdf.axioms.NoAxioms</entry>
</properties>"""  # noqa: E501

_FUSEKI_DATASET = """@prefix :      <#> .
@prefix fuseki: <http://jena.apache.org/fuseki#> .
@prefix tdb2:  <http://jena.apache.org/2016/tdb#> .

:service a fuseki:Service ;
    fuseki:name "tal" ;
    fuseki:endpoint [ fuseki:operation fuseki:query ] ;
    fuseki:endpoint [ fuseki:operation fuseki:update ] ;
    fuseki:endpoint [ fuseki:operation fuseki:gsp-rw ] ;
    fuseki:dataset :dataset .

:dataset a tdb2:DatasetTDB2 ;
    tdb2:location "/fuseki/databases/tal" ;
    tdb2:unionDefaultGraph true .
"""

_GRAPHDB_REPOSITORY = """@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix rep: <http://www.openrdf.org/config/repository#>.
@prefix sr: <http://www.openrdf.org/config/repository/sail#>.
@prefix sail: <http://www.openrdf.org/config/sail#>.
@prefix graphdb: <http://www.ontotext.com/config/graphdb#>.

[] a rep:Repository ;
   rep:repositoryID "tal" ;
   rdfs:label "TAL test repository" ;
   rep:repositoryImpl [
      rep:repositoryType "graphdb:SailRepository" ;
      sr:sailImpl [
         sail:sailType "graphdb:Sail" ;
         graphdb:repository-type "file-repository" ;
      ]
   ].
"""

_LOADED_ENTITIES = (
    "https://github.com/arcangelo7/time_agnostic/br/31830",
    "https://github.com/arcangelo7/time_agnostic/id/27139",
    "https://github.com/arcangelo7/time_agnostic/ar/15519",
)


def _compose(*args: str) -> None:
    subprocess.run(
        ["docker", "compose", "--profile", TRIPLESTORE, *args],
        cwd=TESTS_DIR,
        check=True,
    )


def _qlever(*args: str) -> None:
    subprocess.run(["qlever", *args], cwd=QLEVER_DIR, check=True)


def _virtuoso_isql(statement: str) -> None:
    _compose(
        "exec",
        "-T",
        "virtuoso",
        "/opt/virtuoso-opensource/bin/isql",
        "-U",
        "dba",
        "-P",
        "dba",
        f"exec={statement}",
    )


def _configure_virtuoso() -> None:
    _virtuoso_isql("DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('nobody', 7);")
    _virtuoso_isql("DB.DBA.USER_GRANT_ROLE ('SPARQL', 'SPARQL_UPDATE');")


def _configure_blazegraph() -> None:
    requests.post(
        "http://127.0.0.1:41730/bigdata/namespace",
        data=_BLAZEGRAPH_NAMESPACE,
        headers={"Content-Type": "application/xml"},
        timeout=60,
    ).raise_for_status()


def _configure_fuseki() -> None:
    requests.post(
        "http://127.0.0.1:41740/$/datasets",
        data=_FUSEKI_DATASET,
        auth=("admin", "admin"),
        headers={"Content-Type": "text/turtle"},
        timeout=60,
    ).raise_for_status()


def _configure_graphdb() -> None:
    requests.post(
        "http://127.0.0.1:41750/rest/repositories",
        files={"config": ("tal.ttl", _GRAPHDB_REPOSITORY, "text/turtle")},
        timeout=60,
    ).raise_for_status()


_CONFIGURE = {
    "virtuoso": _configure_virtuoso,
    "blazegraph": _configure_blazegraph,
    "fuseki": _configure_fuseki,
    "graphdb": _configure_graphdb,
}


def prepare_test_data() -> Dataset:
    dataset = fixture_provenance()
    dataset.parse(TESTS_DIR / "kb" / "data.nq", format="nquads")
    associations = generate_qlever_index(
        (str(snapshot), str(update))
        for snapshot, update in dataset.subject_objects(
            URIRef(ProvEntity.iri_has_update_query)
        )
    )
    for snapshot, predicate, word in associations:
        graph = dataset.graph(str(snapshot).split("/se/")[0] + "/")
        graph.add((snapshot, predicate, word))
    return dataset


def _load_data(data: Dataset) -> None:
    with SPARQLClient(UPDATE_ENDPOINT) as client:
        for graph in data.graphs():
            if len(graph) == 0:
                continue
            triples = " . ".join(f"{s.n3()} {p.n3()} {o.n3()}" for s, p, o in graph)
            client.update(
                f"INSERT DATA {{ GRAPH {graph.identifier.n3()} {{ {triples} . }} }}"
            )


def _wait_for_data(timeout: int = 60) -> None:
    deadline = time.time() + timeout
    with SPARQLClient(ENDPOINT) as client:
        for entity in _LOADED_ENTITIES:
            query = f"SELECT ?p ?o WHERE {{ GRAPH ?g {{ <{entity}> ?p ?o }} }}"
            while not client.query(query)["results"]["bindings"]:
                if time.time() > deadline:
                    msg = f"{TRIPLESTORE} did not serve {entity} within {timeout}s"
                    raise TimeoutError(msg)
                time.sleep(1)


def _start_qlever() -> None:
    QLEVER_DIR.mkdir(exist_ok=True)
    prepare_test_data().serialize(QLEVER_DIR / "tal.nq", format="nquads")
    _qlever(
        "index",
        "--name",
        "tal",
        "--format",
        "nq",
        "--input-files",
        "tal.nq",
        "--cat-input-files",
        "cat tal.nq",
        "--settings-json",
        '{"prefixes-external": []}',
        "--system",
        "docker",
        "--overwrite-existing",
    )
    _qlever(
        "start",
        "--name",
        "tal",
        "--description",
        "TAL test dataset",
        "--host-name",
        "localhost",
        "--port",
        "41760",
        "--access-token",
        "",
        "--kill-existing-with-same-port",
        "--system",
        "docker",
    )


def start_triplestore() -> None:
    if TRIPLESTORE == "qlever":
        _start_qlever()
    else:
        _compose("up", "-d", "--wait")
        _CONFIGURE[TRIPLESTORE]()
        _load_data(prepare_test_data())
    _wait_for_data()


def _stop_qlever() -> None:
    containers = subprocess.run(
        ["docker", "ps", "-aq", "--filter", "name=^qlever.server.tal$"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    if containers:
        subprocess.run(["docker", "rm", "-f", *containers], check=True)
    shutil.rmtree(QLEVER_DIR)


def stop_triplestore() -> None:
    if TRIPLESTORE == "qlever":
        _stop_qlever()
    else:
        _compose("down", "-v")
