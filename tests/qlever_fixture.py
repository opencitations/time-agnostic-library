# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from rdflib import Dataset, Literal, URIRef
from rdflib.namespace import XSD

from time_agnostic_library.prov_entity import ProvEntity

BASE = "https://example.org/tal-index/"
START = "2024-01-01T00:00:00+00:00"
END = "2024-02-01T00:00:00+00:00"
PREDICATE = BASE + "Predicate"
OBJECT = BASE + "École東京"


def fixture_provenance() -> Dataset:
    dataset = Dataset(default_union=True)
    bodies = {
        "match": f"<{PREDICATE}> <{OBJECT}>",
        "lower": f"<{BASE}predicate> <{OBJECT}>",
        "namespace": f"<{PREDICATE}> <{OBJECT}/child>",
        "literal": f'<{PREDICATE}> "{OBJECT}"',
        "wrong-position": f"<{OBJECT}> <{PREDICATE}>",
        "literal-only": f'<{PREDICATE}> "{BASE}literal-only"',
        "split": (
            f"<{PREDICATE}> <{BASE}other> . <{BASE}split> <{BASE}other> <{OBJECT}>"
        ),
    }
    for name, body in bodies.items():
        entity = URIRef(BASE + name)
        graph = dataset.graph(URIRef(f"{entity}/prov/"))
        triple = f"<{entity}> {body} ."
        for number, timestamp, operation in (
            (1, START, "INSERT"),
            (2, END, "DELETE"),
        ):
            snapshot = URIRef(f"{entity}/prov/se/{number}")
            graph.add((snapshot, URIRef(ProvEntity.iri_specialization_of), entity))
            graph.add(
                (
                    snapshot,
                    URIRef(ProvEntity.iri_generated_at_time),
                    Literal(timestamp, datatype=XSD.dateTime),
                )
            )
            graph.add(
                (
                    snapshot,
                    URIRef(ProvEntity.iri_has_update_query),
                    Literal(
                        f"{operation} DATA {{ GRAPH <{BASE}data> {{ {triple} }} }}"
                    ),
                )
            )
            if number == 1:
                graph.add(
                    (
                        snapshot,
                        URIRef(ProvEntity.iri_invalidated_at_time),
                        Literal(END, datatype=XSD.dateTime),
                    )
                )
            else:
                graph.add(
                    (
                        snapshot,
                        URIRef(ProvEntity.iri_was_derived_from),
                        URIRef(f"{entity}/prov/se/1"),
                    )
                )
    return dataset
