import argparse
import glob
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
import tracemalloc
from collections.abc import Callable
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
from parse_queries import DM_STEPS, GRANULARITY_CONFIG, build_config, parse_and_generate

sys.setrecursionlimit(5000)

console = Console()

NUM_RUNS = 5
ALL_QUERY_TYPES = ["vm", "dm", "vq"]

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
    except Exception:
        info["cpu_cores"] = os.cpu_count() or 1
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    info["memory_total_kb"] = int(line.split()[1])
                    break
    except Exception:
        pass
    return info


def _measure_query(fn: Callable[[], dict]) -> dict:
    tracemalloc.start()
    tracemalloc.reset_peak()
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    result["time_s"] = elapsed
    result["memory_peak_bytes"] = peak
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
        return {"num_entities": len(result)}
    return _measure_query(fn)


def run_vq_query(sparql: str, config: dict) -> dict:
    def fn() -> dict:
        vq = VersionQuery(sparql, config_dict=config)
        result, _ = vq.run_agnostic_query()
        return {"num_results": sum(len(v) for v in result.values()), "num_versions": len(result)}
    return _measure_query(fn)


def benchmark_queries(
    queries: list[dict],
    config: dict,
    num_runs: int,
    all_results: dict,
    query_type: str,
    output_file: Path,
    skip: int = 0,
) -> None:
    if skip > 0:
        console.print(f"[dim]Resuming from query {skip + 1}/{len(queries)}[/dim]")

    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task("Running queries", total=len(queries), completed=skip)
        for query_spec in queries[skip:]:
            qt = query_spec["type"]
            sparql = query_spec["sparql"]
            on_time = query_spec["on_time"]

            try:
                if qt == "vm":
                    run_vm_query(sparql, tuple(on_time), config)
                elif qt == "dm":
                    run_dm_query(sparql, tuple(on_time), config)
                elif qt == "vq":
                    run_vq_query(sparql, config)
            except Exception as e:
                console.print(f"    [yellow]Warmup error: {e}")

            times = []
            memory_peaks = []
            last_result: dict | None = None
            for run_idx in range(num_runs):
                try:
                    if qt == "vm":
                        last_result = run_vm_query(sparql, tuple(on_time), config)
                    elif qt == "dm":
                        last_result = run_dm_query(sparql, tuple(on_time), config)
                    elif qt == "vq":
                        last_result = run_vq_query(sparql, config)
                    if last_result:
                        times.append(last_result["time_s"])
                        memory_peaks.append(last_result["memory_peak_bytes"])
                except Exception as e:
                    console.print(f"    [red]Run {run_idx + 1} error: {e}")
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
                "mean_memory_bytes": statistics.mean(valid_memory) if valid_memory else None,
                "median_memory_bytes": statistics.median(valid_memory) if valid_memory else None,
                "max_memory_bytes": max(valid_memory) if valid_memory else None,
            }
            if last_result:
                entry["num_results"] = last_result.get("num_results", last_result.get("num_entities", 0))
            all_results["results"][query_type].append(entry)
            save_results(all_results, output_file)
            progress.advance(task)


def save_results(all_results: dict, output_file: Path) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)


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


def find_latest_run(granularity: str) -> Path | None:
    pattern = str(DATA_DIR / f"benchmark_runs_{granularity}_*.json")
    matches = sorted(glob.glob(pattern))
    if matches:
        return Path(matches[-1])
    return None


def create_run_file(granularity: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return DATA_DIR / f"benchmark_runs_{granularity}_{timestamp}.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--granularity", choices=["daily", "hourly", "instant"], default="daily")
    parser.add_argument("--only", choices=ALL_QUERY_TYPES, nargs="+", help="Run only specified query types (e.g. --only vq)")
    parser.add_argument("--runs", type=int, default=NUM_RUNS, help="Number of repetitions per query (default: 5)")
    parser.add_argument("--resume", action="store_true", help="Resume from latest run file, continuing from where it stopped")
    args = parser.parse_args()

    queries_file = DATA_DIR / f"parsed_queries_{args.granularity}.json"
    canonical_file = DATA_DIR / f"benchmark_results_{args.granularity}.json"

    query_types = args.only if args.only else ALL_QUERY_TYPES

    if not queries_file.exists():
        console.print(f"[yellow]Parsed queries not found, generating {queries_file}...")
        gc = GRANULARITY_CONFIG[args.granularity]
        all_queries = parse_and_generate(gc["num_versions"], gc["interval"], DM_STEPS[args.granularity])
        queries_file.parent.mkdir(parents=True, exist_ok=True)
        with open(queries_file, "w", encoding="utf-8") as f:
            json.dump(all_queries, f, indent=2)
        console.print(f"[green]Saved parsed queries to {queries_file}")
    else:
        with open(queries_file, "r", encoding="utf-8") as f:
            all_queries = json.load(f)

    config = build_config(args.granularity)

    hardware = get_hardware_info()
    console.print(f"[bold]Hardware:[/bold] {hardware}")

    if args.resume:
        run_file = find_latest_run(args.granularity)
        if run_file:
            console.print(f"[bold]Resuming from {run_file}[/bold]")
            with open(run_file, "r", encoding="utf-8") as f:
                all_results = json.load(f)
        else:
            console.print("[yellow]No previous run file found, starting fresh[/yellow]")
            run_file = create_run_file(args.granularity)
            all_results = {"hardware": hardware, "results": {}}
    else:
        run_file = create_run_file(args.granularity)
        all_results = {"hardware": hardware, "results": {}}

    for query_type in query_types:
        queries = all_queries.get(query_type, [])
        existing_count = len(all_results["results"].get(query_type, []))

        if existing_count >= len(queries):
            console.print(f"[dim]Skipping {query_type.upper()} ({existing_count}/{len(queries)} already completed)[/dim]")
            continue

        if query_type not in all_results["results"]:
            all_results["results"][query_type] = []

        console.rule(f"[bold]{query_type.upper()} queries[/bold] ({len(queries)} queries, {args.runs} runs each)")
        benchmark_queries(
            queries, config,
            num_runs=args.runs,
            all_results=all_results,
            query_type=query_type,
            output_file=run_file,
            skip=existing_count,
        )
        console.print(f"[green]Saved {query_type.upper()} results to {run_file}[/green]")

    shutil.copy2(run_file, canonical_file)
    console.print(f"\nAll results saved to {canonical_file}")
    print_summary_table(all_results)


if __name__ == "__main__":
    main()
