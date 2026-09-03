# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import hashlib
import re
from collections import Counter, defaultdict
from pathlib import Path

_VERSION_LINE = re.compile(r"^\[Solution in version (\d+)\](.*)$")


def digest_solutions(solutions: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(solutions)).encode()).hexdigest()


def parse_mat_answers(path: Path) -> dict[int, list[str]]:
    answers: dict[int, list[str]] = defaultdict(list)
    with path.open(encoding="utf-8", errors="replace") as file:
        for raw_line in file:
            match = _VERSION_LINE.match(raw_line.rstrip("\n"))
            if match is not None:
                answers[int(match[1])].append(match[2])
    return dict(answers)


def binding_signature(binding: dict, variables: list[str]) -> str:
    if not variables:
        return "true"
    terms = []
    for variable in variables:
        value = binding[variable]
        if value["type"] == "uri":
            terms.append(f"<{value['value']}>")
        elif value["type"] == "bnode":
            terms.append(f"_:{value['value']}")
        else:
            terms.append(value["value"])
    return " ".join(terms)


def summarize(bindings: list[dict], variables: list[str]) -> dict[str, int | str]:
    solutions = [binding_signature(binding, variables) for binding in bindings]
    return {"count": len(solutions), "digest": digest_solutions(solutions)}


def expected_summary(
    answers: dict[int, list[str]], version: int
) -> dict[str, int | str]:
    solutions = answers[version] if version in answers else []
    return {"count": len(solutions), "digest": digest_solutions(solutions)}


def expected_delta(
    answers: dict[int, list[str]], start: int, end: int
) -> dict[str, int | str]:
    start_counter = Counter(answers[start] if start in answers else [])
    end_counter = Counter(answers[end] if end in answers else [])
    additions = list((end_counter - start_counter).elements())
    deletions = list((start_counter - end_counter).elements())
    return {
        "additions": len(additions),
        "deletions": len(deletions),
        "additions_digest": digest_solutions(additions),
        "deletions_digest": digest_solutions(deletions),
    }
