import argparse
import glob
import json
import statistics
import subprocess
import time
import xml.etree.ElementTree as ET
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import TypeVar

import requests
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

console = Console()

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
QUERIES_DIR = DATA_DIR / "queries"

GRAPH_URI = "http://bear.benchmark/dataset"
R43PLES_PORT = 9998
DEFAULT_REPLICATIONS = 1
MAX_RETRIES = 5
RETRY_BACKOFF_S = 5

NUM_VERSIONS_MAP = {"daily": 89, "hourly": 1299, "instant": 21046}
DM_STEPS = {"daily": 5, "hourly": 100, "instant": 1500}

QUERY_FILES = ["p.txt", "po.txt"]

SPARQL_NS = "http://www.w3.org/2005/sparql-results#"

PROGRESS_COLUMNS = (
    SpinnerColumn(),
    TextColumn("[bold blue]{task.description}"),
    BarColumn(),
    MofNCompleteColumn(),
    TimeElapsedColumn(),
    TimeRemainingColumn(),
)


T = TypeVar("T")


def _restart_container(granularity: str) -> None:
    container = f"r43ples-bear-{granularity}"
    console.print(f"[yellow]Restarting container {container}...")
    subprocess.run(["docker", "restart", container], check=True, capture_output=True)
    for i in range(1, 61):
        try:
            resp = requests.get(
                f"http://localhost:{R43PLES_PORT}/r43ples/sparql",
                timeout=2,
            )
            if resp.status_code < 500:
                console.print(f"[green]R43ples ready after {i}s")
                return
        except requests.ConnectionError:
            pass
        time.sleep(1)
    raise RuntimeError("R43ples failed to restart within 60s")


def _with_retry(fn: Callable[[], T], granularity: str) -> T:
    for attempt in range(MAX_RETRIES):
        try:
            return fn()
        except (requests.ConnectionError, requests.Timeout):
            if attempt == MAX_RETRIES - 1:
                raise
            console.print(
                f"[yellow]Connection failed (attempt {attempt + 1}/{MAX_RETRIES}), "
                f"restarting container..."
            )
            _restart_container(granularity)
            time.sleep(RETRY_BACKOFF_S)
    raise RuntimeError("unreachable")


def parse_bear_query_file(filepath: Path) -> list[tuple[str, str, str]]:
    queries = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(" ", 2)
            if len(parts) == 3:
                s, p, o = parts
                if o.endswith(" ."):
                    o = o[:-2]
                elif o.endswith("."):
                    o = o[:-1].strip()
                queries.append((s.strip(), p.strip(), o.strip()))
    return queries


def load_revision_map(granularity: str) -> dict[int, int]:
    map_file = DATA_DIR / f"r43ples_revision_map_{granularity}.json"
    if not map_file.exists():
        console.print("[yellow]No revision map found, assuming 1:1 mapping")
        return {}
    with open(map_file, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return {int(k): v for k, v in raw.items()}


def build_sparql(pattern: tuple[str, str, str], pattern_type: str, revision: int) -> str:
    _, p, o = pattern
    if pattern_type == "p":
        return (
            f'SELECT ?s ?o WHERE {{ GRAPH <{GRAPH_URI}> REVISION "{revision}" '
            f'{{ ?s {p} ?o . }} }}'
        )
    return (
        f'SELECT ?s WHERE {{ GRAPH <{GRAPH_URI}> REVISION "{revision}" '
        f'{{ ?s {p} {o} . }} }}'
    )


def query_r43ples(session: requests.Session, sparql: str, endpoint: str) -> int:
    resp = session.get(
        endpoint,
        params={"query": sparql},
        headers={"Accept": "application/sparql-results+xml"},
        timeout=600,
    )
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    return len(root.findall(f".//{{{SPARQL_NS}}}result"))


def timed_query(session: requests.Session, sparql: str, endpoint: str) -> tuple[float, int]:
    start = time.perf_counter()
    count = query_r43ples(session, sparql, endpoint)
    elapsed = time.perf_counter() - start
    return elapsed, count


def save_state(state: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def count_items(state: dict, query_type: str, pattern_type: str) -> int:
    return sum(
        1 for e in state["detail"][query_type]
        if e["pattern_type"] == pattern_type
    )


def compute_dm_versions(num_versions: int, dm_step: int) -> list[int]:
    diff_versions = list(range(dm_step, min(num_versions, dm_step * 11 + 1), dm_step))
    if num_versions not in diff_versions:
        diff_versions.append(num_versions)
    return diff_versions


def global_warmup(
    session: requests.Session,
    patterns: list[tuple[str, str, str]],
    pattern_type: str,
    num_versions: int,
    endpoint: str,
    revision_map: dict[int, int],
    granularity: str,
) -> None:
    sample_versions = [1, num_versions // 2, num_versions]
    sample_patterns = patterns[:min(3, len(patterns))]
    console.print(f"  Warming up ({len(sample_versions) * len(sample_patterns)} queries)...")
    for v in sample_versions:
        revision = revision_map[v] if revision_map else v
        for pat in sample_patterns:
            sparql = build_sparql(pat, pattern_type, revision)
            _with_retry(lambda: query_r43ples(session, sparql, endpoint), granularity)


def run_vm_benchmark(
    session: requests.Session,
    patterns: list[tuple[str, str, str]],
    pattern_type: str,
    num_versions: int,
    endpoint: str,
    revision_map: dict[int, int],
    num_replications: int,
    state: dict,
    state_file: Path,
    granularity: str,
    skip: int = 0,
) -> None:
    if skip == 0:
        global_warmup(session, patterns, pattern_type, num_versions, endpoint, revision_map, granularity)
    total = num_versions * len(patterns)
    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task(f"VM {pattern_type}", total=total, completed=skip)
        item_idx = 0
        for version in range(1, num_versions + 1):
            revision = revision_map[version] if revision_map else version
            for pat_idx, pattern in enumerate(patterns):
                if item_idx < skip:
                    item_idx += 1
                    continue
                sparql = build_sparql(pattern, pattern_type, revision)
                times = []
                count = 0
                for _ in range(num_replications):
                    elapsed, count = _with_retry(lambda: timed_query(session, sparql, endpoint), granularity)
                    times.append(elapsed)
                median_ms = statistics.median(times) * 1000
                state["detail"]["vm"].append({
                    "pattern_type": pattern_type,
                    "pattern_index": pat_idx,
                    "version": version,
                    "median_ms": median_ms,
                    "results": count,
                })
                save_state(state, state_file)
                item_idx += 1
                progress.advance(task)


def run_dm_benchmark(
    session: requests.Session,
    patterns: list[tuple[str, str, str]],
    pattern_type: str,
    num_versions: int,
    dm_step: int,
    endpoint: str,
    revision_map: dict[int, int],
    num_replications: int,
    state: dict,
    state_file: Path,
    granularity: str,
    skip: int = 0,
) -> None:
    diff_versions = compute_dm_versions(num_versions, dm_step)
    rev_1 = revision_map[1] if revision_map else 1
    if skip == 0:
        global_warmup(session, patterns, pattern_type, num_versions, endpoint, revision_map, granularity)
    total = len(diff_versions) * len(patterns)
    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task(f"DM {pattern_type}", total=total, completed=skip)
        item_idx = 0
        for target_version in diff_versions:
            rev_n = revision_map[target_version] if revision_map else target_version
            for pat_idx, pattern in enumerate(patterns):
                if item_idx < skip:
                    item_idx += 1
                    continue
                sparql_v0 = build_sparql(pattern, pattern_type, rev_1)
                sparql_vn = build_sparql(pattern, pattern_type, rev_n)
                times = []
                count = 0
                for _ in range(num_replications):
                    start = time.perf_counter()
                    results_v0 = _with_retry(lambda: query_r43ples(session, sparql_v0, endpoint), granularity)
                    results_vn = _with_retry(lambda: query_r43ples(session, sparql_vn, endpoint), granularity)
                    elapsed = time.perf_counter() - start
                    count = abs(results_vn - results_v0)
                    times.append(elapsed)
                median_ms = statistics.median(times) * 1000
                state["detail"]["dm"].append({
                    "pattern_type": pattern_type,
                    "pattern_index": pat_idx,
                    "version_start": 1,
                    "version_end": target_version,
                    "median_ms": median_ms,
                    "results": count,
                })
                save_state(state, state_file)
                item_idx += 1
                progress.advance(task)


def run_vq_benchmark(
    session: requests.Session,
    patterns: list[tuple[str, str, str]],
    pattern_type: str,
    num_versions: int,
    endpoint: str,
    revision_map: dict[int, int],
    num_replications: int,
    state: dict,
    state_file: Path,
    granularity: str,
    skip: int = 0,
) -> None:
    if skip == 0:
        global_warmup(session, patterns, pattern_type, num_versions, endpoint, revision_map, granularity)
    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task(f"VQ {pattern_type}", total=len(patterns), completed=skip)
        for pat_idx, pattern in enumerate(patterns):
            if pat_idx < skip:
                continue
            times = []
            total_count = 0
            for _ in range(num_replications):
                start = time.perf_counter()
                run_total = 0
                for version in range(1, num_versions + 1):
                    revision = revision_map[version] if revision_map else version
                    sparql = build_sparql(pattern, pattern_type, revision)
                    run_total += _with_retry(lambda: query_r43ples(session, sparql, endpoint), granularity)
                elapsed = time.perf_counter() - start
                total_count = run_total
                times.append(elapsed)
            median_ms = statistics.median(times) * 1000
            state["detail"]["vq"].append({
                "pattern_type": pattern_type,
                "pattern_index": pat_idx,
                "median_ms": median_ms,
                "results": total_count,
            })
            save_state(state, state_file)
            progress.advance(task)


def build_final_output(state: dict) -> dict:
    ingestion_file = DATA_DIR / f"r43ples_ingestion_time_{state['granularity']}.json"
    ingestion_s = None
    if ingestion_file.exists():
        with open(ingestion_file, "r", encoding="utf-8") as f:
            ingestion_s = json.load(f)["ingestion_s"]

    output: dict = {
        "replications": state["replications"],
        "ingestion_s": ingestion_s,
        "num_versions": state["num_versions"],
        "results": {},
        "detail": {},
    }

    vm_entries = state["detail"]["vm"]
    if vm_entries:
        output["results"]["vm"] = _aggregate_all(vm_entries)
        vm_by_version: dict[int, list] = {}
        for e in vm_entries:
            vm_by_version.setdefault(e["version"], []).append(e)
        output["detail"]["per_version_vm"] = [
            {"version": v, "patterns": pats}
            for v, pats in sorted(vm_by_version.items())
        ]

    dm_entries = state["detail"]["dm"]
    if dm_entries:
        output["results"]["dm"] = _aggregate_all(dm_entries)
        dm_by_delta: dict[int, list] = {}
        for e in dm_entries:
            dm_by_delta.setdefault(e["version_end"], []).append(e)
        output["detail"]["per_delta_dm"] = [
            {"version_start": 1, "version_end": v, "patterns": pats}
            for v, pats in sorted(dm_by_delta.items())
        ]

    vq_entries = state["detail"]["vq"]
    if vq_entries:
        output["results"]["vq"] = _aggregate_all(vq_entries)
        output["detail"]["per_pattern_vq"] = vq_entries

    return output


def _aggregate_by_type(entries: list[dict]) -> dict[str, dict]:
    by_pattern: dict[str, list[float]] = {}
    for e in entries:
        by_pattern.setdefault(e["pattern_type"], []).append(e["median_ms"])
    result = {}
    for pt, vals in by_pattern.items():
        result[pt] = {
            "count": len(vals),
            "mean_ms": statistics.mean(vals),
            "median_ms": statistics.median(vals),
        }
    return result


def _aggregate_all(entries: list[dict]) -> dict:
    medians = [e["median_ms"] for e in entries]
    return {
        "count": len(medians),
        "mean_ms": statistics.mean(medians),
        "median_ms": statistics.median(medians),
        "by_pattern": _aggregate_by_type(entries),
    }


def print_summary(results: dict) -> None:
    table = Table(title="R43ples benchmark results")
    table.add_column("Query type", style="bold")
    table.add_column("Pattern", style="dim")
    table.add_column("Count", justify="right")
    table.add_column("Mean (ms)", justify="right")
    table.add_column("Median (ms)", justify="right")

    for qt in ["vm", "dm", "vq"]:
        data = results.get(qt)
        if not data:
            continue
        table.add_row(
            qt.upper(), "all",
            str(data["count"]),
            f"{data['mean_ms']:.4f}",
            f"{data['median_ms']:.4f}",
        )
        for pt, pt_data in data.get("by_pattern", {}).items():
            table.add_row(
                "", pt,
                str(pt_data["count"]),
                f"{pt_data['mean_ms']:.4f}",
                f"{pt_data['median_ms']:.4f}",
            )

    console.print(table)


def find_latest_run(granularity: str) -> Path | None:
    pattern = str(DATA_DIR / f"r43ples_runs_{granularity}_*.json")
    matches = sorted(glob.glob(pattern))
    if matches:
        return Path(matches[-1])
    return None


def create_run_file(granularity: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return DATA_DIR / f"r43ples_runs_{granularity}_{timestamp}.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--granularity", choices=["daily", "hourly", "instant"], default="daily")
    parser.add_argument("--only", choices=["vm", "dm", "vq"], nargs="+")
    parser.add_argument("--replications", type=int, default=DEFAULT_REPLICATIONS)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    num_versions = NUM_VERSIONS_MAP[args.granularity]
    dm_step = DM_STEPS[args.granularity]
    endpoint = f"http://localhost:{R43PLES_PORT}/r43ples/sparql"
    output_file = DATA_DIR / f"r43ples_benchmark_results_{args.granularity}.json"
    query_types = args.only if args.only else ["vm", "dm", "vq"]
    revision_map = load_revision_map(args.granularity)

    if args.resume:
        run_file = find_latest_run(args.granularity)
        if run_file:
            console.print(f"[bold]Resuming from {run_file}[/bold]")
            with open(run_file, "r", encoding="utf-8") as f:
                state = json.load(f)
        else:
            console.print("[yellow]No previous run file found, starting fresh")
            run_file = create_run_file(args.granularity)
            state = {
                "replications": args.replications,
                "granularity": args.granularity,
                "num_versions": num_versions,
                "detail": {"vm": [], "dm": [], "vq": []},
            }
    else:
        run_file = create_run_file(args.granularity)
        state = {
            "replications": args.replications,
            "granularity": args.granularity,
            "num_versions": num_versions,
            "detail": {"vm": [], "dm": [], "vq": []},
        }

    console.print(f"[bold]R43ples benchmark ({args.granularity}, {num_versions} versions)[/bold]")
    console.print(f"  Endpoint: {endpoint}")
    console.print(f"  Replications: {args.replications} (global warmup per query type)")
    console.print(f"  Run file: {run_file}")
    if revision_map:
        console.print(f"  Revision map: {len(revision_map)} entries (max rev {max(revision_map.values())})")

    session = requests.Session()

    for query_file in QUERY_FILES:
        pattern_type = query_file.replace(".txt", "")
        query_path = QUERIES_DIR / query_file
        if not query_path.exists():
            console.print(f"[yellow]Query file not found: {query_path}")
            continue
        patterns = parse_bear_query_file(query_path)
        console.print(f"Loaded {len(patterns)} {pattern_type} patterns")

        if "vm" in query_types:
            total = num_versions * len(patterns)
            completed = count_items(state, "vm", pattern_type)
            if completed >= total:
                console.print(f"[dim]Skipping VM {pattern_type} ({completed}/{total} already completed)")
            else:
                console.rule(f"[bold]VM ({pattern_type})")
                run_vm_benchmark(
                    session, patterns, pattern_type, num_versions, endpoint,
                    revision_map, args.replications, state, run_file,
                    args.granularity, skip=completed,
                )

        if "dm" in query_types:
            diff_versions = compute_dm_versions(num_versions, dm_step)
            total = len(diff_versions) * len(patterns)
            completed = count_items(state, "dm", pattern_type)
            if completed >= total:
                console.print(f"[dim]Skipping DM {pattern_type} ({completed}/{total} already completed)")
            else:
                console.rule(f"[bold]DM ({pattern_type})")
                run_dm_benchmark(
                    session, patterns, pattern_type, num_versions, dm_step,
                    endpoint, revision_map, args.replications, state, run_file,
                    args.granularity, skip=completed,
                )

        if "vq" in query_types:
            total = len(patterns)
            completed = count_items(state, "vq", pattern_type)
            if completed >= total:
                console.print(f"[dim]Skipping VQ {pattern_type} ({completed}/{total} already completed)")
            else:
                console.rule(f"[bold]VQ ({pattern_type})")
                run_vq_benchmark(
                    session, patterns, pattern_type, num_versions, endpoint,
                    revision_map, args.replications, state, run_file,
                    args.granularity, skip=completed,
                )

    final = build_final_output(state)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(final, f, indent=2)
    console.print(f"\nResults saved to {output_file}")

    print_summary(final["results"])


if __name__ == "__main__":
    main()
