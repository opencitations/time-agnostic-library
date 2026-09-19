# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from collections.abc import Iterable

from rdflib import Literal, URIRef

from time_agnostic_library.agnostic_entity import _fast_parse_update

QLEVER_HAS_WORD = URIRef("http://qlever.cs.uni-freiburg.de/builtin-functions/has-word")


def qlever_search_tokens(triple: tuple[str, ...]) -> set[str]:
    return {
        f"{position}|{term[1:-1]}"
        for position, term in zip(
            ("subject", "predicate", "object"), triple[:3], strict=True
        )
        if term.startswith("<") and term.endswith(">")
    }


def generate_qlever_index(
    snapshot_updates: Iterable[tuple[str, str]],
) -> set[tuple[URIRef, URIRef, Literal]]:
    return {
        (URIRef(snapshot), QLEVER_HAS_WORD, Literal(token))
        for snapshot, update in snapshot_updates
        for _, quads in _fast_parse_update(update)
        for quad in quads
        for token in qlever_search_tokens(quad)
    }
