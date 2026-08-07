# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import json
import re
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from time_agnostic_library.agnostic_query import VersionQuery

sys.path.insert(0, str(Path(__file__).parent))
import corpora
from corpora import Corpus, QuerySet
from parse_queries import load_query_set, to_sparql

sys.setrecursionlimit(5000)

console = Console()

DATA_DIR = Path(__file__).parent / "data"
QUERIES_DIR = DATA_DIR / "queries"

VERSION_LINE_RE = re.compile(r"^\[Solution in version (\d+)\]")


def parse_bear_result_file(filepath: Path) -> dict[int, int]:
    counts = defaultdict(int)
    with filepath.open(encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            m = VERSION_LINE_RE.match(line)
            if m:
                counts[int(m.group(1))] += 1
    return dict(counts)


def expected_counts(
    corpus: Corpus, query_set: QuerySet, atom: str, index: int
) -> dict[int, int] | None:
    if not query_set.results:
        console.print(
            f"  [yellow]BEAR publishes no expected results for {corpus.name}, "
            f"so {query_set.name} cannot be verified"
        )
        return None
    path = corpus.expected_results(query_set, atom, index + 1)
    if not path.exists():
        console.print(f"  [yellow]Result file not found: {path}")
        return None
    return parse_bear_result_file(path)


def run_vm_query(sparql: str, timestamp: str, config: dict) -> tuple[int, float]:
    ts = timestamp
    start = time.perf_counter()
    vq = VersionQuery(
        sparql,
        on_time=(ts, ts),
        merge_aware=False,
        include_prov_metadata=False,
        config_dict=config,
    )
    result, _, _ = vq.run_agnostic_query()
    elapsed = time.perf_counter() - start
    if not result:
        return 0, elapsed
    latest_ts = max(result.keys())
    return len(result[latest_ts]), elapsed


def run_vq_query(sparql: str, config: dict) -> tuple[dict[str, int], float]:
    start = time.perf_counter()
    vq = VersionQuery(
        sparql,
        merge_aware=False,
        include_prov_metadata=False,
        config_dict=config,
    )
    result, _, _ = vq.run_agnostic_query(include_all_timestamps=True)
    elapsed = time.perf_counter() - start
    return {ts: len(bindings) for ts, bindings in result.items()}, elapsed


def verify_pattern_vm(
    pattern_idx: int,
    sparql: str,
    query_set: QuerySet,
    corpus: Corpus,
    config: dict,
) -> list[dict]:
    counts = expected_counts(corpus, query_set, "mat", pattern_idx)
    if counts is None:
        return []

    timestamps = corpus.timestamps()
    results = []

    for version in corpus.sample_versions():
        expected = counts.get(version, 0)
        actual, elapsed = run_vm_query(sparql, timestamps[version], config)
        match = expected == actual
        results.append(
            {
                "query_type": "vm",
                "pattern_type": query_set.name,
                "pattern_index": pattern_idx,
                "version": version,
                "expected": expected,
                "actual": actual,
                "match": match,
                "time_s": elapsed,
            }
        )
        if not match:
            console.print(
                f"  [red]MISMATCH VM {query_set.name}[{pattern_idx}] v{version}: "
                f"expected={expected} actual={actual}"
            )

    return results


def verify_pattern_vq(
    pattern_idx: int,
    sparql: str,
    query_set: QuerySet,
    corpus: Corpus,
    config: dict,
) -> list[dict]:
    counts = expected_counts(corpus, query_set, "ver", pattern_idx)
    if counts is None:
        return []

    actual_by_ts, vq_elapsed = run_vq_query(sparql, config)

    version_of = {ts: version for version, ts in enumerate(corpus.timestamps())}
    actual_by_version = {}
    for ts, count in actual_by_ts.items():
        version = version_of.get(
            datetime.fromisoformat(ts).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        )
        if version is not None:
            actual_by_version[version] = count

    results = []
    all_versions = set(counts.keys()) | set(actual_by_version.keys())
    mismatches = 0
    for version in sorted(all_versions):
        if version >= corpus.num_versions:
            continue
        expected = counts.get(version, 0)
        actual = actual_by_version.get(version, 0)
        match = expected == actual
        results.append(
            {
                "query_type": "vq",
                "pattern_type": query_set.name,
                "pattern_index": pattern_idx,
                "version": version,
                "expected": expected,
                "actual": actual,
                "match": match,
                "time_s": vq_elapsed,
            }
        )
        if not match:
            mismatches += 1

    if mismatches > 0:
        console.print(
            f"  [red]MISMATCH VQ {query_set.name}[{pattern_idx}]: "
            f"{mismatches} versions differ"
        )

    return results


def collect_timing(results: list[dict]) -> dict[str, list[float]]:
    timing: dict[str, list[float]] = defaultdict(list)
    seen_vq: set[tuple[str, int]] = set()
    for r in results:
        if "time_s" not in r:
            continue
        qt = r["query_type"]
        if qt == "vq":
            key = (r["pattern_type"], r["pattern_index"])
            if key in seen_vq:
                continue
            seen_vq.add(key)
        timing[qt].append(r["time_s"])
    return dict(timing)


def print_summary(results: list[dict], baseline: dict | None = None) -> None:
    matched = sum(1 for r in results if r["match"])
    total = len(results)

    table = Table(title="Verification summary")
    table.add_column("Query type", style="bold")
    table.add_column("Pattern", style="bold")
    table.add_column("Matched", justify="right")
    table.add_column("Total", justify="right")
    table.add_column("Status")

    by_group = defaultdict(list)
    for r in results:
        by_group[(r["query_type"], r["pattern_type"])].append(r)

    for (qt, pt), group in sorted(by_group.items()):
        group_matched = sum(1 for r in group if r["match"])
        group_total = len(group)
        status = "[green]PASS" if group_matched == group_total else "[red]FAIL"
        table.add_row(qt.upper(), pt, str(group_matched), str(group_total), status)

    console.print(table)
    console.print(f"\nOverall: {matched}/{total} checks passed")

    timing = collect_timing(results)
    if not timing:
        return

    timing_table = Table(title="Timing summary")
    timing_table.add_column("Query type", style="bold")
    timing_table.add_column("Queries", justify="right")
    timing_table.add_column("Median (ms)", justify="right")
    timing_table.add_column("Mean (ms)", justify="right")
    timing_table.add_column("Min (ms)", justify="right")
    timing_table.add_column("Max (ms)", justify="right")
    if baseline:
        timing_table.add_column("Baseline median (ms)", justify="right")
        timing_table.add_column("Speedup", justify="right")

    for qt in ["vm", "vq"]:
        times = timing.get(qt, [])
        if not times:
            continue
        med = statistics.median(times) * 1000
        mean = statistics.mean(times) * 1000
        row = [
            qt.upper(),
            str(len(times)),
            f"{med:.1f}",
            f"{mean:.1f}",
            f"{min(times) * 1000:.1f}",
            f"{max(times) * 1000:.1f}",
        ]
        if baseline:
            base_times = baseline.get(qt, [])
            if base_times:
                base_med = statistics.median(base_times) * 1000
                speedup = base_med / med if med > 0 else float("inf")
                color = (
                    "[green]" if speedup > 1.05 else "[red]" if speedup < 0.95 else ""
                )
                row.extend([f"{base_med:.1f}", f"{color}{speedup:.2f}x"])
            else:
                row.extend(["N/A", "N/A"])
        timing_table.add_row(*row)

    console.print()
    console.print(timing_table)


def load_baseline(path: Path) -> dict[str, list[float]] | None:
    if not path.exists():
        return None
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def save_timing(timing: dict[str, list[float]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(timing, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--corpus", choices=corpora.CORPUS_NAMES, default="bear-b-daily"
    )
    parser.add_argument(
        "--save-baseline",
        action="store_true",
        help="Save timing as baseline for future comparison",
    )
    parser.add_argument(
        "--compare", action="store_true", help="Compare against saved baseline"
    )
    args = parser.parse_args()

    corpus = corpora.get(args.corpus)
    baseline_file = DATA_DIR / f"timing_baseline_{corpus.name}.json"
    config = corpora.build_config(corpus)

    all_patterns = []
    for query_set in corpus.queries:
        if not query_set.path.exists():
            console.print(f"[yellow]Query file not found: {query_set.path}")
            continue
        for idx, query in enumerate(load_query_set(query_set)):
            all_patterns.append((idx, to_sparql(query), query_set))

    all_results = []
    passed = 0
    failed = 0

    total_steps = len(all_patterns) * 2
    with Progress(
        TextColumn("[bold]{task.fields[label]}"),
        BarColumn(),
        TextColumn("{task.fields[patterns]}"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        TextColumn("{task.fields[status]}"),
        console=console,
    ) as progress:
        task = progress.add_task(
            "Verifying",
            total=total_steps,
            label="",
            patterns=f"0/{len(all_patterns)}",
            status="",
        )

        for pattern_idx, sparql, query_set in all_patterns:
            status_text = f"[green]{passed}[/green] OK  [red]{failed}[/red] FAIL"
            progress.update(
                task, label=f"{query_set.name}[{pattern_idx}] VM", status=status_text
            )
            vm_results = verify_pattern_vm(
                pattern_idx, sparql, query_set, corpus, config
            )
            vm_ok = all(r["match"] for r in vm_results) if vm_results else True
            all_results.extend(vm_results)
            progress.advance(task)

            progress.update(task, label=f"{query_set.name}[{pattern_idx}] VQ")
            vq_results = verify_pattern_vq(
                pattern_idx, sparql, query_set, corpus, config
            )
            vq_ok = all(r["match"] for r in vq_results) if vq_results else True
            all_results.extend(vq_results)
            progress.advance(task)

            if vm_ok and vq_ok:
                passed += 1
            else:
                failed += 1

            progress.update(
                task,
                patterns=f"{passed + failed}/{len(all_patterns)}",
                status=f"[green]{passed}[/green] OK  [red]{failed}[/red] FAIL",
            )

    console.rule("[bold]Final results")

    baseline = load_baseline(baseline_file) if args.compare else None
    if args.compare and baseline is None:
        console.print(
            f"[yellow]No baseline found at {baseline_file}, "
            "run with --save-baseline first"
        )

    print_summary(all_results, baseline=baseline)

    timing = collect_timing(all_results)
    if args.save_baseline:
        save_timing(timing, baseline_file)
        console.print(f"\nBaseline saved to {baseline_file}")

    output_file = DATA_DIR / "verification_results.json"
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)
    console.print(f"\nDetailed results saved to {output_file}")


if __name__ == "__main__":
    main()
