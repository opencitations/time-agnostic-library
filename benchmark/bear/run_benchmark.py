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

sys.setrecursionlimit(5000)

console = Console()

NUM_RUNS = 5
QUERY_TYPES = ["vm", "dm", "vq"]

DATA_DIR = Path(__file__).parent / "data"
QUERIES_FILE = DATA_DIR / "parsed_queries.json"
CONFIG_FILE = Path(__file__).parent / "config_benchmark.json"
OUTPUT_FILE = DATA_DIR / "benchmark_results.json"

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


def get_hardware_info() -> dict:
    info = {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
    }
    try:
        result = subprocess.run(["nproc"], capture_output=True, text=True, check=True)
        info["cpu_cores"] = int(result.stdout.strip())
    except Exception:
        info["cpu_cores"] = os.cpu_count()
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


def benchmark_queries(queries: List[dict], config: dict) -> List[dict]:
    results = []

    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task("Running queries", total=len(queries))
        for query_spec in queries:
            query_type = query_spec["type"]
            sparql = query_spec["sparql"]
            on_time = query_spec.get("on_time")

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
            last_result = None
            for run_idx in range(NUM_RUNS):
                try:
                    if query_type == "vm":
                        last_result = run_vm_query(sparql, tuple(on_time), config)
                    elif query_type == "dm":
                        last_result = run_dm_query(sparql, tuple(on_time), config)
                    elif query_type == "vq":
                        last_result = run_vq_query(sparql, config)
                    times.append(last_result["time_s"])
                except Exception as e:
                    console.print(f"    [red]Run {run_idx + 1} error: {e}")
                    times.append(None)

            valid_times = [t for t in times if t is not None]
            entry = {
                **query_spec,
                "runs": NUM_RUNS,
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


def print_summary_table(all_results: dict) -> None:
    table = Table(title="Benchmark summary")
    table.add_column("Query type", style="bold")
    table.add_column("Queries", justify="right")
    table.add_column("Mean (ms)", justify="right")
    table.add_column("Median (ms)", justify="right")

    for query_type in QUERY_TYPES:
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


def main():
    with open(QUERIES_FILE, "r", encoding="utf-8") as f:
        all_queries = json.load(f)

    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)

    hardware = get_hardware_info()
    console.print(f"[bold]Hardware:[/bold] {hardware}")

    all_results = {"hardware": hardware, "config_path": str(CONFIG_FILE), "results": {}}

    for query_type in QUERY_TYPES:
        queries = all_queries.get(query_type, [])
        console.rule(f"[bold]{query_type.upper()} queries[/bold] ({len(queries)} queries, {NUM_RUNS} runs each)")
        results = benchmark_queries(queries, config)
        all_results["results"][query_type] = results

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)
    console.print(f"\nResults saved to {OUTPUT_FILE}")

    print_summary_table(all_results)


if __name__ == "__main__":
    main()
