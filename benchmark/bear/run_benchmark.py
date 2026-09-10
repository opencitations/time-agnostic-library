# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import json
import statistics
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
from sparqlite import SPARQLError

from time_agnostic_library.agnostic_query import DeltaQuery, VersionQuery

sys.path.insert(0, str(Path(__file__).parent))
import corpora
from answer_sets import binding_signature, digest_solutions
from parse_queries import generate
from protocol import (
    DEFAULT_REPLICATIONS,
    DEFAULT_TIMEOUT_S,
    build_manifest,
    fuseki_info,
    hardware_info,
    manifest_hash,
    protocol_metadata,
)

sys.setrecursionlimit(5000)

console = Console()

NUM_RUNS = DEFAULT_REPLICATIONS
ALL_QUERY_TYPES = ["vm", "sd", "cv"]

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


def _measure_query(fn: Callable[[], dict], measurement: str | None) -> dict:
    if measurement is None:
        return fn()
    if measurement == "time":
        start = time.perf_counter()
        result = fn()
        result["time_s"] = time.perf_counter() - start
        return result
    tracemalloc.start()
    baseline = tracemalloc.get_traced_memory()[0]
    try:
        result = fn()
        _, peak = tracemalloc.get_traced_memory()
        result["memory_peak_bytes"] = peak - baseline
        return result
    finally:
        tracemalloc.stop()


def _digest(bindings: list[dict], variables: list[str]) -> str:
    signatures = [binding_signature(binding, variables) for binding in bindings]
    return digest_solutions(signatures)


def run_vm_query(
    sparql: str,
    variables: list[str],
    on_time: tuple,
    config: dict,
    measurement: str | None,
) -> dict:
    def fn() -> dict:
        vq = VersionQuery(
            sparql,
            on_time=on_time,
            merge_aware=False,
            include_prov_metadata=False,
            config_dict=config,
        )
        result, _, _ = vq.run_agnostic_query()
        return {"result": result}

    measured = _measure_query(fn, measurement)
    result = measured.pop("result")
    buckets = list(result)
    if buckets != [on_time[0]]:
        message = f"VM returned buckets {buckets} for {on_time[0]}"
        raise ValueError(message)
    bindings = result[on_time[0]]
    measured.update(
        {
            "num_results": len(bindings),
            "digest": _digest(bindings, variables),
        }
    )
    return measured


def run_sd_query(
    sparql: str,
    variables: list[str],
    on_time: tuple,
    config: dict,
    measurement: str | None,
) -> dict:
    def fn() -> dict:
        dq = DeltaQuery(
            sparql,
            on_time=on_time,
            merge_aware=False,
            include_prov_metadata=False,
            config_dict=config,
        )
        result, _, _ = dq.run_agnostic_query()
        return result

    measured = _measure_query(fn, measurement)
    additions = measured.pop("additions")
    deletions = measured.pop("deletions")
    measured.pop("changes")
    measured.pop("merges")
    measured.update(
        {
            "additions": len(additions),
            "deletions": len(deletions),
            "additions_digest": _digest(additions, variables),
            "deletions_digest": _digest(deletions, variables),
        }
    )
    return measured


def run_cv_query(
    sparql: str,
    variables: list[str],
    timestamps: Sequence[str],
    config: dict,
    measurement: str | None,
) -> dict:
    def fn() -> dict:
        vq = VersionQuery(
            sparql,
            merge_aware=False,
            include_prov_metadata=False,
            config_dict=config,
        )
        result, _, _ = vq.run_agnostic_query()
        return {"history": result}

    measured = _measure_query(fn, measurement)
    history = sorted(measured.pop("history").items())
    state: list[dict] = []
    position = 0
    versions = {}
    for timestamp in timestamps:
        while position < len(history) and history[position][0] <= timestamp:
            state = history[position][1]
            position += 1
        versions[timestamp] = {
            "count": len(state),
            "digest": _digest(state, variables),
        }
    measured.update(
        {
            "num_results": sum(version["count"] for version in versions.values()),
            "num_versions": len(versions),
            "versions": versions,
        }
    )
    return measured


def _dispatch_query(
    qt: str,
    sparql: str,
    variables: list[str],
    on_time: Sequence[str] | None,
    config: dict,
    measurement: str | None,
    timestamps: Sequence[str] | None = None,
) -> dict | None:
    if qt == "cv":
        if timestamps is None:
            message = "CV requires the corpus timestamps"
            raise ValueError(message)
        return run_cv_query(sparql, variables, timestamps, config, measurement)
    assert on_time is not None
    if qt == "vm":
        return run_vm_query(sparql, variables, tuple(on_time), config, measurement)
    if qt == "sd":
        return run_sd_query(sparql, variables, tuple(on_time), config, measurement)
    return None


def _try_query(
    qt: str,
    sparql: str,
    variables: list[str],
    on_time: Sequence[str] | None,
    config: dict,
    label: str,
    measurement: str | None,
    expected: dict | None = None,
    timestamps: Sequence[str] | None = None,
) -> tuple[dict | None, dict | None]:
    try:
        result = _dispatch_query(
            qt, sparql, variables, on_time, config, measurement, timestamps
        )
    except (SPARQLError, ValueError) as error:
        console.print(f"    {label} error: {error}")
        return None, {
            "type": type(error).__name__,
            "message": str(error),
        }
    if result is not None and expected is not None:
        actual = _correctness_summary(result)
        if actual != expected:
            message = f"{label} answer mismatch: expected={expected}, actual={actual}"
            raise ValueError(message)
    return result, None


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


def _result_summary(result: dict, measurement: str) -> dict:
    metric = "time_s" if measurement == "time" else "memory_peak_bytes"
    return {key: value for key, value in result.items() if key != metric}


def _correctness_summary(result: dict) -> dict:
    return {
        key: value
        for key, value in result.items()
        if key not in {"time_s", "memory_peak_bytes"}
    }


def _entry_status(errors: list[dict | None], summaries: list[dict | None]) -> str:
    valid_summaries = [summary for summary in summaries if summary is not None]
    if not valid_summaries:
        return "failed"
    if any(error is not None for error in errors):
        return "partial"
    if any(summary != valid_summaries[0] for summary in valid_summaries[1:]):
        return "mismatch"
    return "ok"


def append_journal(journal_file: Path, query_type: str, entry: dict) -> None:
    journal_file.parent.mkdir(parents=True, exist_ok=True)
    with journal_file.open("a", encoding="utf-8") as file:
        file.write(json.dumps({"query_type": query_type, "entry": entry}))
        file.write("\n")


def merge_journal(all_results: dict, journal_file: Path) -> None:
    if not journal_file.exists():
        return
    entries_by_type = {
        query_type: {query_key(entry): entry for entry in entries}
        for query_type, entries in all_results["results"].items()
    }
    with journal_file.open(encoding="utf-8") as file:
        lines = file.readlines()
        for index, line in enumerate(lines):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                if index == len(lines) - 1 and not line.endswith("\n"):
                    break
                raise
            query_type = record["query_type"]
            entry = record["entry"]
            entries_by_type.setdefault(query_type, {})[query_key(entry)] = entry
    all_results["results"] = {
        query_type: list(entries.values())
        for query_type, entries in entries_by_type.items()
    }


def benchmark_queries(
    queries: list[dict],
    config: dict,
    num_runs: int,
    all_results: dict,
    query_type: str,
    output_file: Path,
    journal_file: Path,
    measurement: str,
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
            variables = query_spec["variables"]
            on_time = query_spec["on_time"]
            expected = query_spec["expected"]
            timestamps = (
                query_spec["timestamps"] if "timestamps" in query_spec else None
            )

            _try_query(
                qt,
                sparql,
                variables,
                on_time,
                config,
                "[yellow]Warmup",
                None,
                expected,
                timestamps,
            )

            times = []
            memory_peaks = []
            errors: list[dict | None] = []
            summaries: list[dict | None] = []
            for run_idx in range(num_runs):
                result, error = _try_query(
                    qt,
                    sparql,
                    variables,
                    on_time,
                    config,
                    f"[red]Run {run_idx + 1}",
                    measurement,
                    expected,
                    timestamps,
                )
                errors.append(error)
                if result is None:
                    summaries.append(None)
                    times.append(None)
                    memory_peaks.append(None)
                    continue
                summaries.append(_result_summary(result, measurement))
                if measurement == "time":
                    times.append(result["time_s"])
                else:
                    memory_peaks.append(result["memory_peak_bytes"])

            valid_times = [t for t in times if t is not None]
            valid_memory = [m for m in memory_peaks if m is not None]
            entry = {
                **query_spec,
                "runs": num_runs,
                "status": _entry_status(errors, summaries),
                "errors": errors,
                "result_summaries": summaries,
            }
            if measurement == "time":
                entry.update(
                    {
                        "times_s": times,
                        "mean_s": statistics.mean(valid_times) if valid_times else None,
                        "std_s": statistics.stdev(valid_times)
                        if len(valid_times) > 1
                        else 0.0,
                        "median_s": statistics.median(valid_times)
                        if valid_times
                        else None,
                    }
                )
            else:
                entry.update(
                    {
                        "memory_peak_bytes": memory_peaks,
                        "mean_memory_bytes": statistics.mean(valid_memory)
                        if valid_memory
                        else None,
                        "median_memory_bytes": statistics.median(valid_memory)
                        if valid_memory
                        else None,
                        "max_memory_bytes": max(valid_memory) if valid_memory else None,
                    }
                )
            valid_summaries = [summary for summary in summaries if summary is not None]
            if valid_summaries:
                last_summary = valid_summaries[-1]
                if "additions" in last_summary:
                    entry["num_results"] = (
                        last_summary["additions"] + last_summary["deletions"]
                    )
                elif "num_results" in last_summary:
                    entry["num_results"] = last_summary["num_results"]
            all_results["results"][query_type].append(entry)
            append_journal(journal_file, query_type, entry)
            if position % SAVE_EVERY == 0:
                save_results(all_results, output_file)
            progress.advance(task)
    save_results(all_results, output_file)


def save_results(all_results: dict, output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = output_file.with_suffix(f"{output_file.suffix}.tmp")
    with temporary_file.open("w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)
    temporary_file.replace(output_file)


def load_or_generate_queries(
    corpus: corpora.Corpus,
    queries_file: Path,
    source_manifest_hash: str,
) -> dict[str, list[dict]]:
    if queries_file.exists():
        with queries_file.open(encoding="utf-8") as file:
            cached = json.load(file)
        if (
            "source_manifest_hash" in cached
            and cached["source_manifest_hash"] == source_manifest_hash
        ):
            return cached["queries"]

    console.print(f"[yellow]Generating parsed queries in {queries_file}...")
    queries = generate(corpus)
    queries_file.parent.mkdir(parents=True, exist_ok=True)
    with queries_file.open("w", encoding="utf-8") as file:
        json.dump(
            {
                "source_manifest_hash": source_manifest_hash,
                "queries": queries,
            },
            file,
            indent=2,
        )
    console.print(f"[green]Saved parsed queries to {queries_file}")
    return queries


def load_compatible_canonical(
    canonical_file: Path, hardware: dict, protocol: dict
) -> dict:
    canonical: dict = {
        "hardware": hardware,
        "protocol": protocol,
        "results": {},
    }
    if not canonical_file.exists():
        return canonical

    with canonical_file.open(encoding="utf-8") as f:
        canonical = json.load(f)
    if (
        "protocol" not in canonical
        or canonical["protocol"] != protocol
        or canonical["hardware"] != hardware
    ):
        msg = f"Canonical results use a different setup: {canonical_file}"
        raise ValueError(msg)
    return canonical


def update_canonical(
    all_results: dict,
    canonical_file: Path,
    query_types: list[str],
    *,
    merge_existing: bool,
) -> None:
    canonical = (
        load_compatible_canonical(
            canonical_file,
            all_results["hardware"],
            all_results["protocol"],
        )
        if merge_existing
        else {
            "hardware": all_results["hardware"],
            "protocol": all_results["protocol"],
            "results": {},
        }
    )
    for query_type in query_types:
        results = all_results["results"].get(query_type)
        if results:
            canonical["results"][query_type] = results
    save_results(canonical, canonical_file)


def print_summary_table(all_results: dict, measurement: str) -> None:
    table = Table(title="Benchmark summary")
    table.add_column("Query type", style="bold")
    table.add_column("Queries", justify="right")
    unit = "ms" if measurement == "time" else "bytes"
    table.add_column(f"Mean ({unit})", justify="right")
    table.add_column(f"Median ({unit})", justify="right")

    for query_type in ALL_QUERY_TYPES:
        results = all_results["results"].get(query_type, [])
        key = "median_s" if measurement == "time" else "median_memory_bytes"
        valid_means = [
            result[key]
            for result in results
            if result["status"] == "ok" and result[key] is not None
        ]
        if valid_means:
            scale = 1000 if measurement == "time" else 1
            table.add_row(
                query_type.upper(),
                str(len(results)),
                f"{statistics.mean(valid_means) * scale:.2f}",
                f"{statistics.median(valid_means) * scale:.2f}",
            )

    console.print()
    console.print(table)


def find_latest_run(corpus_name: str, run_label: str) -> Path | None:
    matches = sorted(DATA_DIR.glob(f"benchmark_runs_{corpus_name}_{run_label}_*.json"))
    if matches:
        return matches[-1]
    return None


def create_run_file(corpus_name: str, run_label: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return DATA_DIR / f"benchmark_runs_{corpus_name}_{run_label}_{timestamp}.json"


def canonical_path(corpus_name: str, measurement: str) -> Path:
    measurement_part = "" if measurement == "time" else "_memory"
    return DATA_DIR / f"benchmark{measurement_part}_results_{corpus_name}.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--corpus", choices=corpora.CORPUS_NAMES, default="bear-b-daily"
    )
    parser.add_argument(
        "--only",
        choices=ALL_QUERY_TYPES,
        nargs="+",
        help="Run only specified query types (e.g. --only cv)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        help="Number of repetitions per query (default: 3 for time, 1 for memory)",
    )
    parser.add_argument(
        "--measurement",
        choices=["time", "memory"],
        default="time",
        help="Measure elapsed time or traced Python memory",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_S,
        help="SPARQL request timeout in seconds",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from latest run file, continuing from where it stopped",
    )
    args = parser.parse_args()

    num_runs = (
        args.runs
        if args.runs is not None
        else (NUM_RUNS if args.measurement == "time" else 1)
    )
    if num_runs < 1:
        parser.error("--runs must be at least 1")
    if args.timeout < 1:
        parser.error("--timeout must be at least 1")

    corpus = corpora.get(args.corpus)
    queries_file = DATA_DIR / f"parsed_queries_{corpus.name}.json"
    manifest = build_manifest(corpus)
    source_manifest_hash = manifest_hash(manifest)
    canonical_file = canonical_path(corpus.name, args.measurement)
    run_label = args.measurement
    protocol = protocol_metadata(
        manifest,
        num_runs,
        measurement=args.measurement,
        sparql_request_timeout_s=args.timeout,
    )
    protocol["system"] = "tal"
    protocol["store"] = fuseki_info(corpus)

    query_types = args.only or ALL_QUERY_TYPES
    merge_existing = set(query_types) != set(ALL_QUERY_TYPES)

    all_queries = load_or_generate_queries(
        corpus,
        queries_file,
        source_manifest_hash,
    )
    config = corpora.build_config(corpus, timeout_s=args.timeout)

    hardware = hardware_info()
    if merge_existing:
        load_compatible_canonical(canonical_file, hardware, protocol)
    console.print(f"[bold]Hardware:[/bold] {hardware}")
    console.print(
        "[bold]Protocol:[/bold] "
        f"{num_runs} repetitions, {args.measurement}, "
        f"timeout {args.timeout}s, manifest {protocol['manifest_hash']}"
    )

    if args.resume:
        run_file = find_latest_run(corpus.name, run_label)
        if run_file:
            console.print(f"[bold]Resuming from {run_file}[/bold]")
            with run_file.open(encoding="utf-8") as f:
                all_results = json.load(f)
            if all_results["protocol"] != protocol:
                parser.error("The latest run uses a different benchmark protocol")
            if all_results["hardware"] != hardware:
                parser.error("The latest run was measured on different hardware")
            merge_journal(all_results, run_file.with_suffix(".jsonl"))
        else:
            console.print("[yellow]No previous run file found, starting fresh[/yellow]")
            run_file = create_run_file(corpus.name, run_label)
            all_results = {
                "hardware": hardware,
                "protocol": protocol,
                "results": {},
            }
    else:
        run_file = create_run_file(corpus.name, run_label)
        all_results = {
            "hardware": hardware,
            "protocol": protocol,
            "results": {},
        }
    save_results(all_results, run_file)
    journal_file = run_file.with_suffix(".jsonl")

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
            f"({len(queries)} queries, {num_runs} runs each)"
        )
        benchmark_queries(
            pending,
            config,
            num_runs=num_runs,
            all_results=all_results,
            query_type=query_type,
            output_file=run_file,
            journal_file=journal_file,
            measurement=args.measurement,
            total=len(queries),
        )
        console.print(
            f"[green]Saved {query_type.upper()} results to {run_file}[/green]"
        )

    update_canonical(
        all_results,
        canonical_file,
        query_types,
        merge_existing=merge_existing,
    )
    console.print(f"\nAll results saved to {canonical_file}")
    print_summary_table(all_results, args.measurement)


if __name__ == "__main__":
    main()
