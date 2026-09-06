# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import json
import sys
from pathlib import Path

from rich.console import Console

from time_agnostic_library.agnostic_query import DeltaQuery, VersionQuery

sys.path.insert(0, str(Path(__file__).parent))
import corpora
from answer_sets import (
    binding_signature,
    digest_solutions,
    expected_delta,
    expected_summary,
    parse_mat_answers,
    summarize,
)
from parse_queries import load_query_set, to_sparql

console = Console()
DATA_DIR = Path(__file__).parent / "data"


def _variables(pattern: tuple[str, str, str]) -> list[str]:
    return [term[1:] for term in pattern if term.startswith("?")]


def _assert_equal(label: str, actual: object, expected: object) -> None:
    if actual != expected:
        message = f"{label} mismatch: expected={expected}, actual={actual}"
        raise ValueError(message)


def _verify_vm(
    sparql: str,
    variables: list[str],
    answers: dict[int, list[str]],
    corpus: corpora.Corpus,
    config: dict,
) -> list[dict]:
    records = []
    timestamps = corpus.timestamps()
    for version in (0, corpus.num_versions - 1):
        timestamp = timestamps[version]
        result, _, _ = VersionQuery(
            sparql,
            on_time=(timestamp, timestamp),
            config_dict=config,
        ).run_agnostic_query()
        if list(result) != [timestamp]:
            message = f"VM v{version} returned buckets {list(result)}"
            raise ValueError(message)
        actual = summarize(result[timestamp], variables)
        expected = expected_summary(answers, version)
        _assert_equal(f"VM v{version}", actual, expected)
        records.append({"query_type": "vm", "version": version, **actual})
    return records


def _verify_sd(
    sparql: str,
    variables: list[str],
    answers: dict[int, list[str]],
    corpus: corpora.Corpus,
    config: dict,
) -> dict:
    timestamps = corpus.timestamps()
    result, _, _ = DeltaQuery(
        sparql,
        on_time=(timestamps[0], timestamps[-1]),
        config_dict=config,
    ).run_agnostic_query()
    additions = [
        binding_signature(binding, variables) for binding in result["additions"]
    ]
    deletions = [
        binding_signature(binding, variables) for binding in result["deletions"]
    ]
    actual = {
        "additions": len(additions),
        "deletions": len(deletions),
        "additions_digest": digest_solutions(additions),
        "deletions_digest": digest_solutions(deletions),
    }
    expected = expected_delta(answers, 0, corpus.num_versions - 1)
    _assert_equal("SD", actual, expected)
    return {"query_type": "sd", **actual}


def _verify_cv(
    sparql: str,
    variables: list[str],
    answers: dict[int, list[str]],
    corpus: corpora.Corpus,
    config: dict,
) -> dict:
    result, _, _ = VersionQuery(sparql, config_dict=config).run_agnostic_query()
    history = sorted(result.items())
    state: list[dict] = []
    position = 0
    versions = {}
    for version, timestamp in enumerate(corpus.timestamps()):
        while position < len(history) and history[position][0] <= timestamp:
            state = history[position][1]
            position += 1
        actual = summarize(state, variables)
        expected = expected_summary(answers, version)
        _assert_equal(f"CV v{version}", actual, expected)
        versions[str(version)] = actual
    return {"query_type": "cv", "versions": versions}


def verify_corpus(corpus: corpora.Corpus, config: dict) -> list[dict]:
    records = []
    for query_set in corpus.queries:
        for index, pattern in enumerate(load_query_set(query_set)):
            answer_path = corpus.expected_results(query_set, "mat", index + 1)
            if not answer_path.exists():
                message = f"Missing Mat answer set: {answer_path}"
                raise FileNotFoundError(message)
            answers = parse_mat_answers(answer_path)
            sparql = to_sparql(pattern)
            variables = _variables(pattern)
            prefix = {"pattern_type": query_set.name, "pattern_index": index}
            records.extend(
                {**prefix, **record}
                for record in _verify_vm(sparql, variables, answers, corpus, config)
            )
            records.append(
                {
                    **prefix,
                    **_verify_sd(sparql, variables, answers, corpus, config),
                }
            )
            records.append(
                {
                    **prefix,
                    **_verify_cv(sparql, variables, answers, corpus, config),
                }
            )
            console.print(f"[green]PASS[/green] {query_set.name}[{index}]")
    return records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", choices=corpora.CORPUS_NAMES, required=True)
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="Timeout for one SPARQL request in seconds",
    )
    args = parser.parse_args()
    corpus = corpora.get(args.corpus)
    records = verify_corpus(
        corpus,
        corpora.build_config(corpus, timeout_s=args.timeout),
    )
    output = DATA_DIR / f"verification_results_{corpus.name}.json"
    with output.open("w", encoding="utf-8") as file:
        json.dump(records, file, indent=2)
    console.print(f"[green]Verified {len(records)} cases[/green]")


if __name__ == "__main__":
    main()
