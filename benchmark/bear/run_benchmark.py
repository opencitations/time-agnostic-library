import argparse
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import List

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


def get_peak_rss_kb() -> int:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


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


def run_vm_query(sparql: str, on_time: tuple, config: dict) -> dict:
    start = time.perf_counter()
    vq = VersionQuery(sparql, on_time=on_time, config_dict=config)
    result, _ = vq.run_agnostic_query()
    elapsed = time.perf_counter() - start
    num_results = sum(len(v) for v in result.values())
    return {"time_s": elapsed, "num_results": num_results}


def run_dm_query(sparql: str, on_time: tuple, config: dict) -> dict:
    start = time.perf_counter()
    dq = DeltaQuery(sparql, on_time=on_time, config_dict=config)
    result = dq.run_agnostic_query()
    elapsed = time.perf_counter() - start
    num_entities = len(result)
    return {"time_s": elapsed, "num_entities": num_entities}


def run_vq_query(sparql: str, config: dict) -> dict:
    start = time.perf_counter()
    vq = VersionQuery(sparql, config_dict=config)
    result, _ = vq.run_agnostic_query()
    elapsed = time.perf_counter() - start
    num_results = sum(len(v) for v in result.values())
    num_versions = len(result)
    return {"time_s": elapsed, "num_results": num_results, "num_versions": num_versions}


def benchmark_queries(queries: List[dict], config: dict, num_runs: int = NUM_RUNS) -> List[dict]:
    results = []

    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task("Running queries", total=len(queries))
        for query_spec in queries:
            query_type = query_spec["type"]
            sparql = query_spec["sparql"]
            on_time = query_spec["on_time"]

            # Warmup
            try:
                if query_type == "vm":
                    run_vm_query(sparql, tuple(on_time), config)
                elif query_type == "dm":
                    run_dm_query(sparql, tuple(on_time), config)
                elif query_type == "vq":
                    run_vq_query(sparql, config)
            except Exception as e:
                console.print(f"    [yellow]Warmup error: {e}")

            times = []
            last_result: dict | None = None
            for run_idx in range(num_runs):
                try:
                    if query_type == "vm":
                        last_result = run_vm_query(sparql, tuple(on_time), config)
                    elif query_type == "dm":
                        last_result = run_dm_query(sparql, tuple(on_time), config)
                    elif query_type == "vq":
                        last_result = run_vq_query(sparql, config)
                    if last_result:
                        times.append(last_result["time_s"])
                except Exception as e:
                    console.print(f"    [red]Run {run_idx + 1} error: {e}")
                    times.append(None)

            valid_times = [t for t in times if t is not None]
            entry = {
                **query_spec,
                "runs": num_runs,
                "times_s": times,
                "mean_s": statistics.mean(valid_times) if valid_times else None,
                "std_s": statistics.stdev(valid_times) if len(valid_times) > 1 else 0.0,
                "median_s": statistics.median(valid_times) if valid_times else None,
                "peak_rss_kb": get_peak_rss_kb(),
            }
            if last_result:
                entry["num_results"] = last_result.get("num_results", last_result.get("num_entities", 0))
            results.append(entry)
            progress.advance(task)

    return results


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


def load_existing_results(output_file: Path) -> dict | None:
    if output_file.exists():
        with open(output_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--granularity", choices=["daily", "hourly", "instant"], default="daily")
    parser.add_argument("--only", choices=ALL_QUERY_TYPES, nargs="+", help="Run only specified query types (e.g. --only vq)")
    parser.add_argument("--runs", type=int, default=NUM_RUNS, help="Number of repetitions per query (default: 5)")
    parser.add_argument("--resume", action="store_true", help="Resume from existing results, skip already completed query types")
    args = parser.parse_args()

    queries_file = DATA_DIR / f"parsed_queries_{args.granularity}.json"
    output_file = DATA_DIR / f"benchmark_results_{args.granularity}.json"

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

    existing = load_existing_results(output_file) if args.resume else None
    all_results = existing or {"hardware": hardware, "results": {}}

    for query_type in query_types:
        if args.resume and query_type in all_results["results"]:
            console.print(f"[dim]Skipping {query_type.upper()} (already completed)[/dim]")
            continue
        queries = all_queries.get(query_type, [])
        console.rule(f"[bold]{query_type.upper()} queries[/bold] ({len(queries)} queries, {args.runs} runs each)")
        results = benchmark_queries(queries, config, num_runs=args.runs)
        all_results["results"][query_type] = results
        save_results(all_results, output_file)
        console.print(f"[green]Saved {query_type.upper()} results to {output_file}[/green]")

    console.print(f"\nAll results saved to {output_file}")
    print_summary_table(all_results)


if __name__ == "__main__":
    main()
