# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC
# ruff: noqa: E402
# pyright: reportMissingImports=false

import json
import sys
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

BEAR_DIR = Path(__file__).parents[1] / "benchmark" / "bear"
sys.path.insert(0, str(BEAR_DIR))

import analyze_results
import answer_sets
import corpora
import ingest_r43ples
import render_tal_campaign
import run_benchmark
import run_ostrich_benchmark
import run_r43ples_benchmark

from time_agnostic_library.sparql import Sparql


def test_time_and_memory_measurements_are_separate(monkeypatch):
    ticks = iter([10.0, 10.25])
    monkeypatch.setattr(run_benchmark.time, "perf_counter", lambda: next(ticks))

    assert run_benchmark._measure_query(lambda: {"num_results": 2}, "time") == {
        "num_results": 2,
        "time_s": 0.25,
    }

    readings = iter([(100, 100), (140, 180)])
    start = MagicMock()
    stop = MagicMock()
    monkeypatch.setattr(run_benchmark.tracemalloc, "start", start)
    monkeypatch.setattr(run_benchmark.tracemalloc, "stop", stop)
    monkeypatch.setattr(
        run_benchmark.tracemalloc,
        "get_traced_memory",
        lambda: next(readings),
    )

    assert run_benchmark._measure_query(lambda: {"num_results": 2}, "memory") == {
        "num_results": 2,
        "memory_peak_bytes": 80,
    }
    assert start.call_count == 1
    assert stop.call_count == 1


def test_each_case_has_one_unmeasured_warmup(tmp_path, monkeypatch):
    measurements = []

    def try_query(
        _qt,
        _sparql,
        _variables,
        _on_time,
        _config,
        _label,
        measurement,
        _expected,
        _timestamps,
    ):
        measurements.append(measurement)
        if measurement is None:
            return {"num_results": 1}, None
        elapsed = 0.1 if len(measurements) == 2 else 0.2
        return {
            "num_results": 1,
            "time_s": elapsed,
        }, None

    monkeypatch.setattr(run_benchmark, "_try_query", try_query)
    query = {
        "type": "vm",
        "pattern_type": "p",
        "pattern_index": 0,
        "version_index": 0,
        "sparql": "SELECT ?s WHERE { ?s <p> <o> . }",
        "variables": ["s"],
        "on_time": ["start", "end"],
        "expected": {"num_results": 1},
    }
    results = {"results": {"vm": []}}

    run_benchmark.benchmark_queries(
        [query],
        {},
        2,
        results,
        "vm",
        tmp_path / "run.json",
        tmp_path / "run.jsonl",
        "time",
    )

    assert measurements == [None, "time", "time"]
    assert results["results"]["vm"][0]["status"] == "ok"
    assert results["results"]["vm"][0]["times_s"] == [0.1, 0.2]
    assert results["results"]["vm"][0]["result_summaries"] == [
        {"num_results": 1},
        {"num_results": 1},
    ]


def test_mat_answers_drive_counts_digests_and_multiset_deltas(tmp_path):
    answers_file = tmp_path / "mat.txt"
    answers_file.write_text(
        "[Solution in version 0]<https://example.org/a> value\n"
        "[Solution in version 0]<https://example.org/a> value\n"
        "[Solution in version 1]<https://example.org/a> value\n"
        "[Solution in version 1]<https://example.org/b> value\n",
        encoding="utf-8",
    )

    answers = answer_sets.parse_mat_answers(answers_file)

    assert answer_sets.expected_summary(answers, 0) == {
        "count": 2,
        "digest": answer_sets.digest_solutions(
            ["<https://example.org/a> value", "<https://example.org/a> value"]
        ),
    }
    assert answer_sets.expected_delta(answers, 0, 1) == {
        "additions": 1,
        "deletions": 1,
        "additions_digest": answer_sets.digest_solutions(
            ["<https://example.org/b> value"]
        ),
        "deletions_digest": answer_sets.digest_solutions(
            ["<https://example.org/a> value"]
        ),
    }


def test_runner_digest_matches_bear_solution_format():
    bindings = [
        {
            "s": {"type": "uri", "value": "https://example.org/resource"},
            "o": {"type": "literal", "value": "A label", "xml:lang": "en"},
        }
    ]

    assert run_benchmark._digest(bindings, ["s", "o"]) == (
        answer_sets.digest_solutions(["<https://example.org/resource> A label"])
    )
    assert run_benchmark._digest([{}], []) == answer_sets.digest_solutions(["true"])


def test_runner_validates_result_after_timing(monkeypatch):
    events = []
    ticks = iter([10.0, 10.5])
    binding = {"s": {"type": "uri", "value": "https://example.org/resource"}}

    class ObservedResult(dict):
        def __iter__(self):
            events.append("verify")
            return super().__iter__()

    class FakeVersionQuery:
        def __init__(self, *_args, **_kwargs):
            pass

        def run_agnostic_query(self):
            return ObservedResult({"start": [binding]}), {}, {}

    def clock():
        events.append("clock")
        return next(ticks)

    def digest(_bindings, _variables):
        events.append("digest")
        return "answer-digest"

    monkeypatch.setattr(run_benchmark, "VersionQuery", FakeVersionQuery)
    monkeypatch.setattr(run_benchmark.time, "perf_counter", clock)
    monkeypatch.setattr(run_benchmark, "_digest", digest)

    result = run_benchmark.run_vm_query(
        "SELECT ?s WHERE { ?s <p> <o> . }",
        ["s"],
        ("start", "start"),
        {},
        "time",
    )

    assert events == ["clock", "clock", "verify", "digest"]
    assert result == {
        "time_s": 0.5,
        "num_results": 1,
        "digest": "answer-digest",
    }


def test_campaign_memory_table_includes_count_and_mean():
    timed_entries = [
        {"status": "ok", "median_s": 0.1},
        {"status": "ok", "median_s": 0.3},
    ]
    memory_entries = [
        {"status": "ok", "median_memory_bytes": 1_000_000},
        {"status": "ok", "median_memory_bytes": 3_000_000},
    ]
    datasets = {"bear-a": {"results": dict.fromkeys(("vm", "sd", "cv"), timed_entries)}}
    memory = {"bear-a": {"results": dict.fromkeys(("vm", "sd", "cv"), memory_entries)}}

    lines = render_tal_campaign._latex_table(datasets, memory).splitlines()
    start = lines.index(r"\label{tab:memory}")

    assert lines[start : start + 11] == [
        r"\label{tab:memory}",
        r"\begin{tabular}{llrrrrr}",
        r"\toprule",
        r"Dataset & Query & Count & Mean (MB) & Median (MB) & p95 (MB) & Max (MB) \\",
        r"\midrule",
        r"BEAR-A & SV & 2 & 2.0 & 2.0 & 2.9 & 3.0 \\",
        r"BEAR-A & SD & 2 & 2.0 & 2.0 & 2.9 & 3.0 \\",
        r"BEAR-A & CV & 2 & 2.0 & 2.0 & 2.9 & 3.0 \\",
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table*}",
    ]


def test_campaign_rejects_different_time_and_memory_revisions():
    timed = {
        "hardware": {"cpu": "same"},
        "protocol": {
            "manifest_hash": "same",
            "git_dirty": False,
            "git_revision": "time",
            "store": {"name": "same"},
        },
    }
    traced = {
        "hardware": {"cpu": "same"},
        "protocol": {
            "manifest_hash": "same",
            "git_dirty": False,
            "git_revision": "memory",
            "store": {"name": "same"},
        },
    }

    with pytest.raises(
        ValueError,
        match="Time and memory revisions differ for bear-a",
    ):
        render_tal_campaign._validate({"bear-a": timed}, {"bear-a": traced})


def test_journal_replaces_snapshot_entry_and_ignores_truncated_tail(tmp_path):
    old = {"type": "vq", "pattern_type": "p", "pattern_index": 0}
    replacement = {**old, "status": "ok"}
    dm = {
        "type": "dm",
        "pattern_type": "p",
        "pattern_index": 1,
        "version_end": 5,
    }
    results = {"results": {"vq": [old], "dm": []}}
    journal = tmp_path / "run.jsonl"
    run_benchmark.append_journal(journal, "vq", replacement)
    run_benchmark.append_journal(journal, "dm", dm)
    with journal.open("a", encoding="utf-8") as file:
        file.write('{"query_type": "vm"')

    run_benchmark.merge_journal(results, journal)

    assert results == {"results": {"vq": [replacement], "dm": [dm]}}


def test_complete_campaign_replaces_incompatible_canonical(tmp_path):
    canonical_file = tmp_path / "benchmark_results.json"
    canonical_file.write_text(
        json.dumps(
            {
                "hardware": {"cpu": "old"},
                "protocol": {"git_revision": "old"},
                "results": {"vm": [{"status": "old"}]},
            }
        ),
        encoding="utf-8",
    )
    results = {
        "hardware": {"cpu": "current"},
        "protocol": {"git_revision": "current"},
        "results": {
            "vm": [{"status": "ok"}],
            "sd": [{"status": "ok"}],
            "cv": [{"status": "ok"}],
        },
    }

    run_benchmark.update_canonical(
        results,
        canonical_file,
        run_benchmark.ALL_QUERY_TYPES,
        merge_existing=False,
    )

    assert json.loads(canonical_file.read_text(encoding="utf-8")) == results


def test_partial_campaign_rejects_incompatible_canonical(tmp_path):
    canonical_file = tmp_path / "benchmark_results.json"
    canonical_file.write_text(
        json.dumps(
            {
                "hardware": {"cpu": "old"},
                "protocol": {"git_revision": "old"},
                "results": {"vm": [{"status": "ok"}]},
            }
        ),
        encoding="utf-8",
    )
    results = {
        "hardware": {"cpu": "current"},
        "protocol": {"git_revision": "current"},
        "results": {"sd": [{"status": "ok"}]},
    }

    with pytest.raises(ValueError, match="Canonical results use a different setup"):
        run_benchmark.update_canonical(
            results,
            canonical_file,
            ["sd"],
            merge_existing=True,
        )


def test_analysis_merges_matching_memory_and_excludes_invalid_cases():
    time_entry = {
        "type": "vq",
        "pattern_type": "p",
        "pattern_index": 0,
        "status": "ok",
        "mean_s": 0.2,
        "median_s": 0.1,
    }
    invalid_entry = {
        "type": "vq",
        "pattern_type": "p",
        "pattern_index": 1,
        "status": "mismatch",
        "mean_s": 0.001,
        "median_s": 0.001,
    }
    memory_entry = {
        "type": "vq",
        "pattern_type": "p",
        "pattern_index": 0,
        "status": "ok",
        "memory_peak_bytes": [120],
        "mean_memory_bytes": 120,
        "median_memory_bytes": 120,
        "max_memory_bytes": 120,
    }
    time_data = {
        "hardware": {"cpu": "same"},
        "protocol": {"manifest_hash": "same", "git_revision": "same"},
        "results": {"vq": [time_entry, invalid_entry]},
    }
    memory_data = {
        "hardware": {"cpu": "same"},
        "protocol": {"manifest_hash": "same", "git_revision": "same"},
        "results": {"vq": [memory_entry]},
    }

    analyze_results.merge_memory_results(time_data, memory_data)

    assert time_entry == {
        "type": "vq",
        "pattern_type": "p",
        "pattern_index": 0,
        "status": "ok",
        "mean_s": 0.2,
        "median_s": 0.1,
        "memory_peak_bytes": [120],
        "mean_memory_bytes": 120,
        "median_memory_bytes": 120,
        "max_memory_bytes": 120,
    }
    assert analyze_results.compute_aggregates([time_entry, invalid_entry]) == {
        "count": 1,
        "failed_count": 1,
        "mean_ms": 100.0,
        "median_ms": 100.0,
        "std_ms": 0.0,
        "min_ms": 100.0,
        "max_ms": 100.0,
        "mean_memory_bytes": 120,
        "median_memory_bytes": 120,
        "max_memory_bytes": 120,
    }


def test_analysis_rejects_different_time_and_memory_manifests():
    time_data = {"protocol": {"manifest_hash": "time"}, "results": {}}
    memory_data = {"protocol": {"manifest_hash": "memory"}, "results": {}}

    with pytest.raises(
        ValueError,
        match="Time and memory results use different query manifests",
    ):
        analyze_results.merge_memory_results(time_data, memory_data)


def test_cross_system_analysis_requires_the_same_clean_protocol(tmp_path):
    protocol = {
        "manifest_hash": "manifest",
        "replications": 3,
        "per_case_statistic": "median",
        "git_revision": "revision",
        "git_dirty": False,
    }
    comparison_file = tmp_path / "comparison.json"
    comparison_file.write_text(
        json.dumps(
            {
                "hardware": {"cpu": "same"},
                "protocol": {**protocol, "replications": 5},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=f"Benchmark protocol field replications differs in {comparison_file}",
    ):
        analyze_results.validate_manifest(
            {"hardware": {"cpu": "same"}, "protocol": protocol},
            [comparison_file],
        )


def test_cross_system_analysis_checks_each_result_count(tmp_path):
    tal_results = {
        "results": {
            "vm": [
                {
                    "type": "vm",
                    "pattern_type": "p",
                    "pattern_index": 2,
                    "version_index": 4,
                    "status": "ok",
                    "num_results": 7,
                }
            ],
            "vq": [
                {
                    "type": "vq",
                    "pattern_type": "p",
                    "pattern_index": 2,
                    "status": "ok",
                    "num_results": 11,
                }
            ],
        }
    }
    comparison_file = tmp_path / "comparison.json"
    comparison_file.write_text(
        json.dumps(
            {
                "protocol": {"system": "r43ples"},
                "detail": {
                    "per_version_vm": [
                        {
                            "version": 4,
                            "patterns": [
                                {
                                    "pattern_type": "p",
                                    "pattern_index": 2,
                                    "results": 7,
                                }
                            ],
                        }
                    ],
                    "per_pattern_vq": [
                        {
                            "pattern_type": "p",
                            "pattern_index": 2,
                            "results": 12,
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError,
        match=r"Result counts differ .* for 1 cases; first mismatch: "
        r"\('vq', 'p', 2, None\)",
    ):
        analyze_results.validate_result_counts(tal_results, [comparison_file])


def test_r43ples_builds_generic_patterns_and_parses_result_sets():
    assert run_r43ples_benchmark.build_sparql(("<s>", "?p", "?o"), 4) == (
        'SELECT ?p ?o WHERE { GRAPH <http://bear.benchmark/dataset> REVISION "4" '
        "{ <s> ?p ?o . } }"
    )
    assert run_r43ples_benchmark.build_sparql(("<s>", "<p>", "<o>"), 2) == (
        'SELECT * WHERE { GRAPH <http://bear.benchmark/dataset> REVISION "2" '
        "{ <s> <p> <o> . } }"
    )
    xml = """<?xml version="1.0"?>
    <sparql xmlns="http://www.w3.org/2005/sparql-results#">
      <head><variable name="s"/></head>
      <results>
        <result><binding name="s"><uri>http://example.org/a</uri></binding></result>
        <result>
          <binding name="s"><literal xml:lang="en">name</literal></binding>
        </result>
      </results>
    </sparql>"""

    assert run_r43ples_benchmark.parse_result_rows(xml) == {
        (
            (
                "s",
                "{http://www.w3.org/2005/sparql-results#}uri",
                "http://example.org/a",
                (),
            ),
        ),
        (
            (
                "s",
                "{http://www.w3.org/2005/sparql-results#}literal",
                "name",
                (("{http://www.w3.org/XML/1998/namespace}lang", "en"),),
            ),
        ),
    }


def test_r43ples_dm_versions_match_zero_based_tal_endpoints():
    assert run_r43ples_benchmark.compute_dm_versions(12, 5) == [6, 11, 12]


def test_r43ples_journal_recovers_entries_after_the_last_snapshot(tmp_path):
    state_file = tmp_path / "run.json"
    state = {"detail": {"vm": [], "dm": [], "vq": []}}
    entry = {
        "pattern_type": "p",
        "pattern_index": 2,
        "version": 4,
        "median_ms": 1.5,
        "results": 7,
    }
    run_r43ples_benchmark.append_detail(state, "vm", entry, state_file)
    restored = {"detail": {"vm": [], "dm": [], "vq": []}}

    run_r43ples_benchmark.merge_journal(restored, state_file)

    assert restored == {"detail": {"vm": [entry], "dm": [], "vq": []}}


def test_r43ples_ingestion_filters_parser_unsafe_triples_in_batches(tmp_path):
    triples_file = tmp_path / "data.nt"
    triples_file.write_text(
        '<s> <p> "safe value" .\n'
        '<s> <p> "say \\"hello\\"" .\n'
        '<s> <p> "line\\nvalue" .\n'
        '<s> <p> "value {with braces}" .\n',
        encoding="utf-8",
    )
    stats = ingest_r43ples.IngestionStats()

    safe_triples = ingest_r43ples.read_safe_triples(triples_file, stats)

    assert list(ingest_r43ples.triple_batches(safe_triples, batch_size=1)) == [
        ['<s> <p> "safe value" .'],
    ]
    assert stats == ingest_r43ples.IngestionStats(
        accepted=1,
        unsafe_quotes=1,
        escaped_whitespace=1,
        literal_braces=1,
    )
    assert stats.skipped == 3


def test_r43ples_ingestion_propagates_rejected_updates():
    response = MagicMock()
    response.text = "parser error"

    with (
        patch("ingest_r43ples.requests.post", return_value=response),
        pytest.raises(RuntimeError, match="R43ples rejected INSERT: parser error"),
    ):
        ingest_r43ples.send_update("http://example.org", "INSERT", ["<s> <p> <o> ."])


def test_ostrich_query_files_and_dm_results_follow_manifest(tmp_path):
    selected = [
        {"pattern_index": 3, "triple": ["?s", "<p>", "?o"]},
        {"pattern_index": 8, "triple": ["<s>", "?p", "?o"]},
    ]
    query_file = tmp_path / "queries.txt"
    run_ostrich_benchmark.write_query_file(query_file, selected)
    assert query_file.read_text(encoding="utf-8") == ("?s <p> ?o .\n<s> ?p ?o .\n")

    parsed = [
        {
            "dm": [
                {"patch_start": 0, "patch_end": 2},
                {"patch_start": 1, "patch_end": 2},
                {"patch_start": 0, "patch_end": 3},
                {"patch_start": 0, "patch_end": 9},
            ]
        },
        {"dm": []},
    ]
    run_ostrich_benchmark.align_patterns(parsed, selected)
    corpus = corpora.Corpus(
        name="test",
        ic_url="",
        cb_url="",
        num_versions=10,
        interval=timedelta(days=1),
        port=1,
        dm_step=2,
        queries=(),
    )
    run_ostrich_benchmark.filter_dm_workload(parsed, corpus)

    assert parsed == [
        {
            "pattern_index": 3,
            "dm": [
                {"patch_start": 0, "patch_end": 2},
                {"patch_start": 0, "patch_end": 9},
            ],
        },
        {"pattern_index": 8, "dm": []},
    ]


def test_ostrich_aggregates_each_query_case_with_equal_weight():
    patterns = {
        "p": [
            {
                "vm": [{"median_us": 1.0}, {"median_us": 3.0}],
                "dm": [],
                "vq": [],
            },
            {
                "vm": [{"median_us": 8.0}],
                "dm": [],
                "vq": [],
            },
        ]
    }

    assert run_ostrich_benchmark.aggregate_results(patterns) == {
        "vm": {
            "count": 3,
            "mean_us": 4.0,
            "median_us": 3.0,
            "mean_ms": 0.004,
            "median_ms": 0.003,
            "by_pattern": {
                "p": {
                    "count": 3,
                    "mean_us": 4.0,
                    "median_us": 3.0,
                    "mean_ms": 0.004,
                    "median_ms": 0.003,
                }
            },
        }
    }


def test_sparql_client_uses_benchmark_request_policy():
    config = {
        "dataset": {
            "file_paths": [],
            "triplestore_urls": ["http://example.org/sparql"],
        },
        "provenance": {"file_paths": [], "triplestore_urls": []},
        "sparql_timeout": 12,
        "sparql_max_retries": 0,
    }
    client = MagicMock()
    client.query.return_value = {
        "head": {"vars": ["s"]},
        "results": {"bindings": [{"s": {"type": "uri", "value": "urn:s"}}]},
    }

    with patch("time_agnostic_library.sparql._get_client", return_value=client) as get:
        result = Sparql("SELECT ?s WHERE { ?s ?p ?o }", config).run_select_query()

    assert result == {
        "head": {"vars": ["s"]},
        "results": {"bindings": [{"s": {"type": "uri", "value": "urn:s"}}]},
    }
    get.assert_called_once_with("http://example.org/sparql", 0, 0.5, 12)
