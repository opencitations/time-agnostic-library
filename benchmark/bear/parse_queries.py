# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import re

from corpora import Corpus, QuerySet
from rich.console import Console

console = Console()

Query = tuple[str, str, str]

_TRAILING_DOT = re.compile(r"\s*\.\s*$")


def parse_pattern(line: str) -> tuple[str, str, str] | None:
    """Split a line of N-Triples into its three terms.

    Terms are never split on `?`: BEAR-A has subjects whose URI carries a query
    string, and reading variables out of them would corrupt the pattern.
    """
    parts = line.split(" ", 2)
    if len(parts) != 3:
        return None
    subject, predicate, obj = parts
    return subject.strip(), predicate.strip(), _TRAILING_DOT.sub("", obj).strip()


def load_query_set(query_set: QuerySet) -> list[Query]:
    queries = []
    with query_set.path.open(encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            pattern = parse_pattern(line)
            if pattern:
                queries.append(pattern)
    return queries


def to_sparql(query: Query) -> str:
    variables: list[str] = []
    for term in query:
        if term.startswith("?") and term not in variables:
            variables.append(term)
    subject, predicate, obj = query
    # A fully bound pattern, as in BEAR-A spo, projects no variable: the answer
    # is whether the triple holds in that version.
    projection = " ".join(variables) if variables else "*"
    return f"SELECT {projection} WHERE {{ {subject} {predicate} {obj} . }}"


def diff_versions(num_versions: int, dm_step: int) -> list[int]:
    """Endpoints diffed against version 0, one every dm_step versions."""
    versions = list(range(dm_step, num_versions, dm_step))
    last = num_versions - 1
    if last not in versions:
        versions.append(last)
    return versions


def generate(corpus: Corpus) -> dict[str, list[dict]]:
    timestamps = corpus.timestamps()
    all_queries: dict[str, list[dict]] = {"vm": [], "dm": [], "vq": []}

    for query_set in corpus.queries:
        if not query_set.path.exists():
            console.print(f"  [yellow]Query file not found: {query_set.path}")
            continue
        queries = load_query_set(query_set)
        vm_versions = range(corpus.num_versions)
        console.print(
            f"  Parsed {len(queries)} {query_set.name} queries"
            f" ({len(vm_versions)} versions each)"
        )

        for index, query in enumerate(queries):
            sparql = to_sparql(query)
            common = {
                "pattern_type": query_set.name,
                "pattern_index": index,
                "sparql": sparql,
            }
            all_queries["vm"].extend(
                {
                    **common,
                    "type": "vm",
                    "version_index": version,
                    "timestamp": timestamps[version],
                    "on_time": (timestamps[version], timestamps[version]),
                }
                for version in vm_versions
            )
            all_queries["dm"].extend(
                {
                    **common,
                    "type": "dm",
                    "version_start": 0,
                    "version_end": version,
                    "timestamp_start": timestamps[0],
                    "timestamp_end": timestamps[version],
                    "on_time": (timestamps[0], timestamps[version]),
                }
                for version in diff_versions(corpus.num_versions, corpus.dm_step)
            )
            all_queries["vq"].append({**common, "type": "vq", "on_time": None})

    return all_queries
