# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from collections.abc import Iterable

from rdflib import Literal, URIRef

from time_agnostic_library.agnostic_entity import _fast_parse_update

QLEVER_HAS_WORD = URIRef("http://qlever.cs.uni-freiburg.de/builtin-functions/has-word")


def generate_qlever_index(
    snapshot_updates: Iterable[tuple[str, str]],
) -> set[tuple[URIRef, URIRef, Literal]]:
    return {
        (URIRef(snapshot), QLEVER_HAS_WORD, Literal(term[1:-1]))
        for snapshot, update in snapshot_updates
        for _, quads in _fast_parse_update(update)
        for quad in quads
        for term in quad[:3]
        if term.startswith("<") and term.endswith(">")
    }
