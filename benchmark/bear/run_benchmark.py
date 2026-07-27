# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import json
import os
import platform
import statistics
import subprocess
import sys
import time
import tracemalloc
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from pathlib import Path

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from time_agnostic_library.agnostic_query import DeltaQuery, VersionQuery

sys.path.insert(0, str(Path(__file__).parent))
import corpora
from parse_queries import generate

sys.setrecursionlimit(5000)

console = Console()

NUM_RUNS = 5
ALL_QUERY_TYPES = ["vm", "dm", "vq"]

SAVE_EVERY = 200

DATA_DIR = Path(__file__).parent / "data"

PROGRESS_COLUMNS = (
    SpinnerColumn(),
    TextColumn("[bold blue]{task.description}"),
    BarColumn(),
    MofNCompleteColumn(),
    TimeElapsedColumn(),
    TimeRemainingColumn(),
)


def get_hardware_info() -> dict[str, str | int]:
    info: dict[str, str | int] = {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
    }
    try:
        result = subprocess.run(["nproc"], capture_output=True, text=True, check=True)
        info["cpu_cores"] = int(result.stdout.strip())
    except (OSError, subprocess.CalledProcessError, ValueError):
        info["cpu_cores"] = os.cpu_count() or 1
    try:
        with Path("/proc/meminfo").open() as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    info["memory_total_kb"] = int(line.split()[1])
                    break
    except (OSError, ValueError):
        pass
    return info


def _measure_query(fn: Callable[[], dict]) -> dict:
    if not tracemalloc.is_tracing():
        tracemalloc.start()
    tracemalloc.reset_peak()
    baseline = tracemalloc.get_traced_memory()[0]
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    result["time_s"] = elapsed
    result["memory_peak_bytes"] = peak - baseline
    return result


def run_vm_query(sparql: str, on_time: tuple, config: dict) -> dict:
    def fn() -> dict:
        vq = VersionQuery(sparql, on_time=on_time, config_dict=config)
        result, _ = vq.run_agnostic_query()
        return {"num_results": sum(len(v) for v in result.values())}

    return _measure_query(fn)


def run_dm_query(sparql: str, on_time: tuple, config: dict) -> dict:
    def fn() -> dict:
        dq = DeltaQuery(sparql, on_time=on_time, config_dict=config)
        result = dq.run_agnostic_query()
        total_additions = sum(len(v["additions"]) for v in result.values())
        total_deletions = sum(len(v["deletions"]) for v in result.values())
        return {
            "num_entities": len(result),
            "additions": total_additions,
            "deletions": total_deletions,
        }

    return _measure_query(fn)


def run_vq_query(sparql: str, config: dict) -> dict:
    def fn() -> dict:
        vq = VersionQuery(sparql, config_dict=config)
        result, _ = vq.run_agnostic_query()
        return {
            "num_results": sum(len(v) for v in result.values()),
            "num_versions": len(result),
        }

    return _measure_query(fn)


def _dispatch_query(
    qt: str, sparql: str, on_time: Sequence[str] | None, config: dict
) -> dict | None:
    if qt == "vq":
        return run_vq_query(sparql, config)
    assert on_time is not None
    if qt == "vm":
        return run_vm_query(sparql, tuple(on_time), config)
    if qt == "dm":
        return run_dm_query(sparql, tuple(on_time), config)
    return None


def _try_query(
    qt: str, sparql: str, on_time: Sequence[str] | None, config: dict, label: str
) -> dict | None:
    try:
        return _dispatch_query(qt, sparql, on_time, config)
    except Exception as e:  # noqa: BLE001 -- keep benchmarking past any single query failure
        console.print(f"    {label} error: {e}")
        return None


def query_key(spec: dict) -> tuple:
    # Identifies a query across runs, so that resuming does not depend on the
    # position a query happens to have in the list.
    return (
        spec["type"],
        spec.get("pattern_type"),
        spec.get("pattern_index"),
        spec.get("version_index"),
        spec.get("version_end"),
    )


def pending_queries(queries: list[dict], completed: list[dict]) -> list[dict]:
    done = {query_key(entry) for entry in completed}
    return [spec for spec in queries if query_key(spec) not in done]


def benchmark_queries(
    queries: list[dict],
    config: dict,
    num_runs: int,
    all_results: dict,
    query_type: str,
    output_file: Path,
    total: int | None = None,
) -> None:
    total = total if total is not None else len(queries)
    done = total - len(queries)
    if done > 0:
        console.print(f"[dim]Resuming: {len(queries)} queries left of {total}[/dim]")

    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task("Running queries", total=total, completed=done)
        for position, query_spec in enumerate(queries, start=1):
            qt = query_spec["type"]
            sparql = query_spec["sparql"]
            on_time = query_spec["on_time"]

            _try_query(qt, sparql, on_time, config, "[yellow]Warmup")

            times = []
            memory_peaks = []
            last_result: dict | None = None
            for run_idx in range(num_runs):
                result = _try_query(
                    qt, sparql, on_time, config, f"[red]Run {run_idx + 1}"
                )
                if result:
                    last_result = result
                    times.append(result["time_s"])
                    memory_peaks.append(result["memory_peak_bytes"])
                else:
                    times.append(None)
                    memory_peaks.append(None)

            valid_times = [t for t in times if t is not None]
            valid_memory = [m for m in memory_peaks if m is not None]
            entry = {
                **query_spec,
                "runs": num_runs,
                "times_s": times,
                "mean_s": statistics.mean(valid_times) if valid_times else None,
                "std_s": statistics.stdev(valid_times) if len(valid_times) > 1 else 0.0,
                "median_s": statistics.median(valid_times) if valid_times else None,
                "memory_peak_bytes": memory_peaks,
                "mean_memory_bytes": statistics.mean(valid_memory)
                if valid_memory
                else None,
                "median_memory_bytes": statistics.median(valid_memory)
                if valid_memory
                else None,
                "max_memory_bytes": max(valid_memory) if valid_memory else None,
            }
            if last_result:
                entry["num_results"] = last_result.get(
                    "num_results", last_result.get("num_entities", 0)
                )
            all_results["results"][query_type].append(entry)
            if position % SAVE_EVERY == 0:
                save_results(all_results, output_file)
            progress.advance(task)
    save_results(all_results, output_file)


def save_results(all_results: dict, output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)


def update_canonical(
    all_results: dict, canonical_file: Path, query_types: list[str]
) -> None:
    canonical: dict = {"hardware": all_results["hardware"], "results": {}}
    if canonical_file.exists():
        with canonical_file.open(encoding="utf-8") as f:
            canonical = json.load(f)
        canonical["hardware"] = all_results["hardware"]
    for query_type in query_types:
        results = all_results["results"].get(query_type)
        if results:
            canonical["results"][query_type] = results
    save_results(canonical, canonical_file)


def print_summary_table(all_results: dict) -> None:
    table = Table(title="Benchmark summary")
    table.add_column("Query type", style="bold")
    table.add_column("Queries", justify="right")
    table.add_column("Mean (ms)", justify="right")
    table.add_column("Median (ms)", justify="right")

    for query_type in ALL_QUERY_TYPES:
        results = all_results["results"].get(query_type, [])
        valid_means = [r["mean_s"] for r in results if r["mean_s"] is not None]
        if valid_means:
            table.add_row(
                query_type.upper(),
                str(len(results)),
                f"{statistics.mean(valid_means) * 1000:.2f}",
                f"{statistics.median(valid_means) * 1000:.2f}",
            )

    console.print()
    console.print(table)


def find_latest_run(corpus_name: str) -> Path | None:
    matches = sorted(DATA_DIR.glob(f"benchmark_runs_{corpus_name}_*.json"))
    if matches:
        return matches[-1]
    return None


def create_run_file(corpus_name: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return DATA_DIR / f"benchmark_runs_{corpus_name}_{timestamp}.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--corpus", choices=corpora.CORPUS_NAMES, default="bear-b-daily"
    )
    parser.add_argument(
        "--only",
        choices=ALL_QUERY_TYPES,
        nargs="+",
        help="Run only specified query types (e.g. --only vq)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=NUM_RUNS,
        help="Number of repetitions per query (default: 5)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from latest run file, continuing from where it stopped",
    )
    args = parser.parse_args()

    corpus = corpora.get(args.corpus)
    queries_file = DATA_DIR / f"parsed_queries_{corpus.name}.json"
    canonical_file = DATA_DIR / f"benchmark_results_{corpus.name}.json"

    query_types = args.only or ALL_QUERY_TYPES

    if not queries_file.exists():
        console.print(f"[yellow]Parsed queries not found, generating {queries_file}...")
        all_queries = generate(corpus)
        queries_file.parent.mkdir(parents=True, exist_ok=True)
        with queries_file.open("w", encoding="utf-8") as f:
            json.dump(all_queries, f, indent=2)
        console.print(f"[green]Saved parsed queries to {queries_file}")
    else:
        with queries_file.open(encoding="utf-8") as f:
            all_queries = json.load(f)

    config = corpora.build_config(corpus)

    hardware = get_hardware_info()
    console.print(f"[bold]Hardware:[/bold] {hardware}")

    if args.resume:
        run_file = find_latest_run(corpus.name)
        if run_file:
            console.print(f"[bold]Resuming from {run_file}[/bold]")
            with run_file.open(encoding="utf-8") as f:
                all_results = json.load(f)
        else:
            console.print("[yellow]No previous run file found, starting fresh[/yellow]")
            run_file = create_run_file(corpus.name)
            all_results = {"hardware": hardware, "results": {}}
    else:
        run_file = create_run_file(corpus.name)
        all_results = {"hardware": hardware, "results": {}}

    for query_type in query_types:
        queries = all_queries.get(query_type, [])
        completed = all_results["results"].setdefault(query_type, [])
        pending = pending_queries(queries, completed)

        if not pending:
            console.print(
                f"[dim]Skipping {query_type.upper()} "
                f"({len(completed)}/{len(queries)} already completed)[/dim]"
            )
            continue

        console.rule(
            f"[bold]{query_type.upper()} queries[/bold] "
            f"({len(queries)} queries, {args.runs} runs each)"
        )
        benchmark_queries(
            pending,
            config,
            num_runs=args.runs,
            all_results=all_results,
            query_type=query_type,
            output_file=run_file,
            total=len(queries),
        )
        console.print(
            f"[green]Saved {query_type.upper()} results to {run_file}[/green]"
        )

    update_canonical(all_results, canonical_file, query_types)
    console.print(f"\nAll results saved to {canonical_file}")
    print_summary_table(all_results)


if __name__ == "__main__":
    main()
