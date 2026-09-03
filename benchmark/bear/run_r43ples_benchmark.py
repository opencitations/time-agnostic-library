# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import json
import statistics
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import corpora
import requests
from protocol import (
    DEFAULT_REPLICATIONS,
    DEFAULT_TIMEOUT_S,
    build_manifest,
    docker_image_id,
    hardware_info,
    manifest_patterns,
    protocol_metadata,
)
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
GRAPH_URI = "http://bear.benchmark/dataset"
R43PLES_PORT = 9998
SAVE_EVERY = 200

SPARQL_NS = "http://www.w3.org/2005/sparql-results#"

PROGRESS_COLUMNS = (
    SpinnerColumn(),
    TextColumn("[bold blue]{task.description}"),
    BarColumn(),
    MofNCompleteColumn(),
    TimeElapsedColumn(),
    TimeRemainingColumn(),
)


ResultBinding = tuple[str, str, str, tuple[tuple[str, str], ...]]
ResultRow = tuple[ResultBinding, ...]
PatternRecord = tuple[int, tuple[str, str, str]]


def load_revision_map(corpus_name: str) -> dict[int, int]:
    map_file = DATA_DIR / f"r43ples_revision_map_{corpus_name}.json"
    with map_file.open(encoding="utf-8") as f:
        raw = json.load(f)
    return {int(k): v for k, v in raw.items()}


def build_sparql(pattern: tuple[str, str, str], revision: int) -> str:
    variables = []
    for term in pattern:
        if term.startswith("?") and term not in variables:
            variables.append(term)
    projection = " ".join(variables) if variables else "*"
    subject, predicate, obj = pattern
    return (
        f'SELECT {projection} WHERE {{ GRAPH <{GRAPH_URI}> REVISION "{revision}" '
        f"{{ {subject} {predicate} {obj} . }} }}"
    )


def parse_result_rows(response_text: str) -> set[ResultRow]:
    root = ET.fromstring(response_text)
    rows = set()
    for result in root.findall(f".//{{{SPARQL_NS}}}result"):
        bindings = []
        for binding in result:
            value = binding[0]
            bindings.append(
                (
                    binding.attrib["name"],
                    value.tag,
                    value.text or "",
                    tuple(sorted(value.attrib.items())),
                )
            )
        rows.add(tuple(sorted(bindings)))
    return rows


def query_r43ples(
    session: requests.Session,
    sparql: str,
    endpoint: str,
    timeout_s: int,
    *,
    query_rewriting: bool,
) -> set[ResultRow]:
    params = {"query": sparql}
    if query_rewriting:
        params["query_rewriting"] = "true"
    resp = session.get(
        endpoint,
        params=params,
        headers={"Accept": "application/sparql-results+xml"},
        timeout=timeout_s,
    )
    resp.raise_for_status()
    return parse_result_rows(resp.text)


def timed_query(
    session: requests.Session,
    sparql: str,
    endpoint: str,
    timeout_s: int,
    *,
    query_rewriting: bool,
) -> tuple[float, int]:
    start = time.perf_counter()
    count = query_r43ples(
        session,
        sparql,
        endpoint,
        timeout_s,
        query_rewriting=query_rewriting,
    )
    elapsed = time.perf_counter() - start
    return elapsed, len(count)


def save_state(state: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = path.with_suffix(f"{path.suffix}.tmp")
    with temporary_file.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    temporary_file.replace(path)


def detail_key(query_type: str, entry: dict) -> tuple:
    version = None
    if query_type == "vm":
        version = entry["version"]
    elif query_type == "dm":
        version = entry["version_end"]
    return (
        entry["pattern_type"],
        entry["pattern_index"],
        version,
    )


def append_detail(state: dict, query_type: str, entry: dict, state_file: Path) -> None:
    state["detail"][query_type].append(entry)
    journal_file = state_file.with_suffix(".jsonl")
    with journal_file.open("a", encoding="utf-8") as file:
        file.write(json.dumps({"query_type": query_type, "entry": entry}))
        file.write("\n")
    completed = sum(len(entries) for entries in state["detail"].values())
    if completed % SAVE_EVERY == 0:
        save_state(state, state_file)


def merge_journal(state: dict, state_file: Path) -> None:
    journal_file = state_file.with_suffix(".jsonl")
    if not journal_file.exists():
        return
    entries_by_type = {
        query_type: {detail_key(query_type, entry): entry for entry in entries}
        for query_type, entries in state["detail"].items()
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
            entries_by_type[query_type][detail_key(query_type, entry)] = entry
    state["detail"] = {
        query_type: list(entries.values())
        for query_type, entries in entries_by_type.items()
    }


def count_items(state: dict, query_type: str, pattern_type: str) -> int:
    return sum(
        1 for e in state["detail"][query_type] if e["pattern_type"] == pattern_type
    )


def compute_dm_versions(num_versions: int, dm_step: int) -> list[int]:
    diff_versions = [index + 1 for index in range(dm_step, num_versions, dm_step)]
    if num_versions not in diff_versions:
        diff_versions.append(num_versions)
    return diff_versions


def run_vm_benchmark(
    session: requests.Session,
    patterns: list[PatternRecord],
    pattern_type: str,
    num_versions: int,
    endpoint: str,
    revision_map: dict[int, int],
    num_replications: int,
    state: dict,
    state_file: Path,
    timeout_s: int,
    *,
    query_rewriting: bool,
    skip: int = 0,
) -> None:
    total = num_versions * len(patterns)
    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task(f"VM {pattern_type}", total=total, completed=skip)
        item_idx = 0
        for version in range(1, num_versions + 1):
            revision = revision_map[version]
            for pattern_index, pattern in patterns:
                if item_idx < skip:
                    item_idx += 1
                    continue
                sparql = build_sparql(pattern, revision)
                query_r43ples(
                    session,
                    sparql,
                    endpoint,
                    timeout_s,
                    query_rewriting=query_rewriting,
                )
                times = []
                counts = []
                for _ in range(num_replications):
                    elapsed, count = timed_query(
                        session,
                        sparql,
                        endpoint,
                        timeout_s,
                        query_rewriting=query_rewriting,
                    )
                    times.append(elapsed)
                    counts.append(count)
                if any(count != counts[0] for count in counts[1:]):
                    msg = "R43ples returned different VM results across replications"
                    raise RuntimeError(msg)
                median_ms = statistics.median(times) * 1000
                append_detail(
                    state,
                    "vm",
                    {
                        "pattern_type": pattern_type,
                        "pattern_index": pattern_index,
                        "version": version - 1,
                        "median_ms": median_ms,
                        "results": counts[0],
                    },
                    state_file,
                )
                item_idx += 1
                progress.advance(task)
    save_state(state, state_file)


def run_dm_benchmark(
    session: requests.Session,
    patterns: list[PatternRecord],
    pattern_type: str,
    num_versions: int,
    dm_step: int,
    endpoint: str,
    revision_map: dict[int, int],
    num_replications: int,
    state: dict,
    state_file: Path,
    timeout_s: int,
    *,
    query_rewriting: bool,
    skip: int = 0,
) -> None:
    diff_versions = compute_dm_versions(num_versions, dm_step)
    rev_1 = revision_map[1]
    total = len(diff_versions) * len(patterns)
    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task(f"DM {pattern_type}", total=total, completed=skip)
        item_idx = 0
        for target_version in diff_versions:
            rev_n = revision_map[target_version]
            for pattern_index, pattern in patterns:
                if item_idx < skip:
                    item_idx += 1
                    continue
                sparql_v0 = build_sparql(pattern, rev_1)
                sparql_vn = build_sparql(pattern, rev_n)
                query_r43ples(
                    session,
                    sparql_v0,
                    endpoint,
                    timeout_s,
                    query_rewriting=query_rewriting,
                )
                query_r43ples(
                    session,
                    sparql_vn,
                    endpoint,
                    timeout_s,
                    query_rewriting=query_rewriting,
                )
                times = []
                counts = []
                for _ in range(num_replications):
                    start = time.perf_counter()
                    results_v0 = query_r43ples(
                        session,
                        sparql_v0,
                        endpoint,
                        timeout_s,
                        query_rewriting=query_rewriting,
                    )
                    results_vn = query_r43ples(
                        session,
                        sparql_vn,
                        endpoint,
                        timeout_s,
                        query_rewriting=query_rewriting,
                    )
                    elapsed = time.perf_counter() - start
                    count = len(results_v0.symmetric_difference(results_vn))
                    times.append(elapsed)
                    counts.append(count)
                if any(count != counts[0] for count in counts[1:]):
                    msg = "R43ples returned different DM results across replications"
                    raise RuntimeError(msg)
                median_ms = statistics.median(times) * 1000
                append_detail(
                    state,
                    "dm",
                    {
                        "pattern_type": pattern_type,
                        "pattern_index": pattern_index,
                        "version_start": 0,
                        "version_end": target_version - 1,
                        "median_ms": median_ms,
                        "results": counts[0],
                    },
                    state_file,
                )
                item_idx += 1
                progress.advance(task)
    save_state(state, state_file)


def query_all_versions(
    session: requests.Session,
    pattern: tuple[str, str, str],
    num_versions: int,
    endpoint: str,
    revision_map: dict[int, int],
    timeout_s: int,
    *,
    query_rewriting: bool,
) -> int:
    total = 0
    for version in range(1, num_versions + 1):
        revision = revision_map[version]
        sparql = build_sparql(pattern, revision)
        total += len(
            query_r43ples(
                session,
                sparql,
                endpoint,
                timeout_s,
                query_rewriting=query_rewriting,
            )
        )
    return total


def run_vq_benchmark(
    session: requests.Session,
    patterns: list[PatternRecord],
    pattern_type: str,
    num_versions: int,
    endpoint: str,
    revision_map: dict[int, int],
    num_replications: int,
    state: dict,
    state_file: Path,
    timeout_s: int,
    *,
    query_rewriting: bool,
    skip: int = 0,
) -> None:
    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task(
            f"VQ {pattern_type}", total=len(patterns), completed=skip
        )
        for position, (pattern_index, pattern) in enumerate(patterns):
            if position < skip:
                continue
            query_all_versions(
                session,
                pattern,
                num_versions,
                endpoint,
                revision_map,
                timeout_s,
                query_rewriting=query_rewriting,
            )
            times = []
            counts = []
            for _ in range(num_replications):
                start = time.perf_counter()
                run_total = query_all_versions(
                    session,
                    pattern,
                    num_versions,
                    endpoint,
                    revision_map,
                    timeout_s,
                    query_rewriting=query_rewriting,
                )
                elapsed = time.perf_counter() - start
                counts.append(run_total)
                times.append(elapsed)
            if any(count != counts[0] for count in counts[1:]):
                msg = "R43ples returned different VQ results across replications"
                raise RuntimeError(msg)
            median_ms = statistics.median(times) * 1000
            append_detail(
                state,
                "vq",
                {
                    "pattern_type": pattern_type,
                    "pattern_index": pattern_index,
                    "median_ms": median_ms,
                    "results": counts[0],
                },
                state_file,
            )
            progress.advance(task)
    save_state(state, state_file)


def build_final_output(state: dict) -> dict:
    ingestion_file = DATA_DIR / f"r43ples_ingestion_time_{state['corpus_name']}.json"
    ingestion_s = None
    if ingestion_file.exists():
        with ingestion_file.open(encoding="utf-8") as f:
            ingestion_s = json.load(f)["ingestion_s"]

    output: dict = {
        "hardware": state["hardware"],
        "protocol": state["protocol"],
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
            {"version_start": 0, "version_end": v, "patterns": pats}
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
            qt.upper(),
            "all",
            str(data["count"]),
            f"{data['mean_ms']:.4f}",
            f"{data['median_ms']:.4f}",
        )
        for pt, pt_data in data.get("by_pattern", {}).items():
            table.add_row(
                "",
                pt,
                str(pt_data["count"]),
                f"{pt_data['mean_ms']:.4f}",
                f"{pt_data['median_ms']:.4f}",
            )

    console.print(table)


def find_latest_run(corpus_name: str, run_label: str) -> Path | None:
    matches = sorted(DATA_DIR.glob(f"r43ples_runs_{corpus_name}_{run_label}_*.json"))
    if matches:
        return matches[-1]
    return None


def create_run_file(corpus_name: str, run_label: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return DATA_DIR / f"r43ples_runs_{corpus_name}_{run_label}_{timestamp}.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--corpus", choices=corpora.CORPUS_NAMES, default="bear-b-daily"
    )
    parser.add_argument("--only", choices=["vm", "dm", "vq"], nargs="+")
    parser.add_argument("--replications", type=int, default=DEFAULT_REPLICATIONS)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--query-rewriting", action="store_true")
    args = parser.parse_args()
    if args.replications < 1:
        parser.error("--replications must be at least 1")
    if args.timeout < 1:
        parser.error("--timeout must be at least 1")

    corpus = corpora.get(args.corpus)
    num_versions = corpus.num_versions
    dm_step = corpus.dm_step
    endpoint = f"http://localhost:{R43PLES_PORT}/r43ples/sparql"
    mode = "query_rewriting" if args.query_rewriting else "default"
    manifest = build_manifest(corpus)
    mode_part = "" if mode == "default" else f"_{mode}"
    output_file = DATA_DIR / f"r43ples_benchmark_results_{args.corpus}{mode_part}.json"
    run_label = mode
    protocol = protocol_metadata(
        manifest,
        args.replications,
        measurement="time",
        sparql_request_timeout_s=args.timeout,
    )
    protocol["system"] = "r43ples"
    protocol["engine_image_id"] = docker_image_id("plttud/r43ples:latest")
    protocol["query_rewriting"] = args.query_rewriting
    protocol["dm_execution"] = "symmetric difference of endpoint result sets"
    protocol["vq_execution"] = "one materialization request per revision"
    protocol["reported_version_indexing"] = "zero-based"
    ingestion_stats_file = DATA_DIR / f"r43ples_ingestion_stats_{args.corpus}.json"
    with ingestion_stats_file.open(encoding="utf-8") as file:
        protocol["ingestion_stats"] = json.load(file)
    protocol["ingestion_filter"] = "r43ples_safe_triples"
    query_types = args.only or ["vm", "dm", "vq"]
    revision_map = load_revision_map(args.corpus)
    if sorted(revision_map) != list(range(1, num_versions + 1)):
        parser.error("The R43ples revision map does not cover every corpus version")
    hardware = hardware_info()

    if args.resume:
        run_file = find_latest_run(args.corpus, run_label)
        if run_file:
            console.print(f"[bold]Resuming from {run_file}[/bold]")
            with run_file.open(encoding="utf-8") as f:
                state = json.load(f)
            if state["protocol"] != protocol:
                parser.error("The latest run uses a different benchmark protocol")
            if state["hardware"] != hardware:
                parser.error("The latest run was measured on different hardware")
            merge_journal(state, run_file)
        else:
            console.print("[yellow]No previous run file found, starting fresh")
            run_file = create_run_file(args.corpus, run_label)
            state = {
                "hardware": hardware,
                "protocol": protocol,
                "replications": args.replications,
                "corpus_name": args.corpus,
                "num_versions": num_versions,
                "detail": {"vm": [], "dm": [], "vq": []},
            }
    else:
        run_file = create_run_file(args.corpus, run_label)
        state = {
            "hardware": hardware,
            "protocol": protocol,
            "replications": args.replications,
            "corpus_name": args.corpus,
            "num_versions": num_versions,
            "detail": {"vm": [], "dm": [], "vq": []},
        }
    save_state(state, run_file)

    console.print(
        f"[bold]R43ples benchmark ({args.corpus}, {num_versions} versions)[/bold]"
    )
    console.print(f"  Endpoint: {endpoint}")
    console.print(f"  Query rewriting: {args.query_rewriting}")
    console.print(f"  Replications: {args.replications} (one warmup per case)")
    console.print(f"  Timeout: {args.timeout}s")
    console.print(f"  Manifest: {protocol['manifest_hash']}")
    console.print(f"  Run file: {run_file}")
    console.print(
        f"  Revision map: {len(revision_map)} entries "
        f"(max rev {max(revision_map.values())})"
    )

    session = requests.Session()

    for query_set in corpus.queries:
        pattern_type = query_set.name
        patterns = [
            (record["pattern_index"], tuple(record["triple"]))
            for record in manifest_patterns(manifest, pattern_type)
        ]
        console.print(f"Loaded {len(patterns)} {pattern_type} patterns")

        if "vm" in query_types:
            total = num_versions * len(patterns)
            completed = count_items(state, "vm", pattern_type)
            if completed >= total:
                console.print(
                    f"[dim]Skipping VM {pattern_type} "
                    f"({completed}/{total} already completed)"
                )
            else:
                console.rule(f"[bold]VM ({pattern_type})")
                run_vm_benchmark(
                    session,
                    patterns,
                    pattern_type,
                    num_versions,
                    endpoint,
                    revision_map,
                    args.replications,
                    state,
                    run_file,
                    args.timeout,
                    query_rewriting=args.query_rewriting,
                    skip=completed,
                )

        if "dm" in query_types:
            diff_versions = compute_dm_versions(num_versions, dm_step)
            total = len(diff_versions) * len(patterns)
            completed = count_items(state, "dm", pattern_type)
            if completed >= total:
                console.print(
                    f"[dim]Skipping DM {pattern_type} "
                    f"({completed}/{total} already completed)"
                )
            else:
                console.rule(f"[bold]DM ({pattern_type})")
                run_dm_benchmark(
                    session,
                    patterns,
                    pattern_type,
                    num_versions,
                    dm_step,
                    endpoint,
                    revision_map,
                    args.replications,
                    state,
                    run_file,
                    args.timeout,
                    query_rewriting=args.query_rewriting,
                    skip=completed,
                )

        if "vq" in query_types:
            total = len(patterns)
            completed = count_items(state, "vq", pattern_type)
            if completed >= total:
                console.print(
                    f"[dim]Skipping VQ {pattern_type} "
                    f"({completed}/{total} already completed)"
                )
            else:
                console.rule(f"[bold]VQ ({pattern_type})")
                run_vq_benchmark(
                    session,
                    patterns,
                    pattern_type,
                    num_versions,
                    endpoint,
                    revision_map,
                    args.replications,
                    state,
                    run_file,
                    args.timeout,
                    query_rewriting=args.query_rewriting,
                    skip=completed,
                )

    final = build_final_output(state)
    save_state(final, output_file)
    console.print(f"\nResults saved to {output_file}")

    print_summary(final["results"])


if __name__ == "__main__":
    main()
