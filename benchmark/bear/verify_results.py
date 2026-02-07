import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

from rich.console import Console
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.table import Table

from time_agnostic_library.agnostic_query import VersionQuery

sys.setrecursionlimit(5000)

console = Console()

DATA_DIR = Path(__file__).parent / "data"
RESULTS_DIR = DATA_DIR / "daily" / "results"
QUERIES_DIR = DATA_DIR / "queries"
CONFIG_FILE = Path(__file__).parent / "config_benchmark.json"

NUM_VERSIONS = 89
BASE_TIMESTAMP = datetime.fromisoformat("2015-08-01T00:00:00+00:00")
INTERVAL = timedelta(days=1)

SAMPLE_VERSIONS = [0, 44, 88]

VERSION_LINE_RE = re.compile(r"^\[Solution in version (\d+)\]")


def parse_bear_result_file(filepath: Path) -> Dict[int, int]:
    counts = defaultdict(int)
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            m = VERSION_LINE_RE.match(line)
            if m:
                counts[int(m.group(1))] += 1
    return dict(counts)


def parse_bear_query_file(filepath: Path) -> List[Tuple[str, str, str]]:
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


def bear_pattern_to_sparql(pattern: Tuple[str, str, str], pattern_type: str) -> str:
    _, p, o = pattern
    if pattern_type == "p":
        return f"SELECT ?s ?o WHERE {{ ?s {p} ?o . }}"
    if pattern_type == "po":
        return f"SELECT ?s WHERE {{ ?s {p} {o} . }}"
    raise ValueError(f"Unknown pattern type: {pattern_type}")


def version_to_timestamp(version: int) -> str:
    return (BASE_TIMESTAMP + INTERVAL * version).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def run_vm_query(sparql: str, version: int, config: dict) -> int:
    ts = version_to_timestamp(version)
    vq = VersionQuery(sparql, on_time=(ts, ts), config_dict=config)
    result, _ = vq.run_agnostic_query()
    if not result:
        return 0
    latest_ts = max(result.keys())
    return len(result[latest_ts])


def run_vq_query(sparql: str, config: dict) -> Dict[str, int]:
    vq = VersionQuery(sparql, config_dict=config)
    result, _ = vq.run_agnostic_query(include_all_timestamps=True)
    return {ts: len(bindings) for ts, bindings in result.items()}


def verify_pattern_vm(
    pattern_idx: int,
    pattern: Tuple[str, str, str],
    pattern_type: str,
    config: dict,
) -> List[dict]:
    result_prefix = f"mat-{pattern_type}-queries"
    result_dir = RESULTS_DIR / pattern_type / "mat" / result_prefix
    file_idx = pattern_idx + 1
    result_file = result_dir / f"{result_prefix}-{file_idx}.txt"

    if not result_file.exists():
        console.print(f"  [yellow]Result file not found: {result_file}")
        return []

    expected_counts = parse_bear_result_file(result_file)
    sparql = bear_pattern_to_sparql(pattern, pattern_type)
    results = []

    for version in SAMPLE_VERSIONS:
        expected = expected_counts.get(version, 0)
        actual = run_vm_query(sparql, version, config)
        match = expected == actual
        results.append({
            "query_type": "vm",
            "pattern_type": pattern_type,
            "pattern_index": pattern_idx,
            "version": version,
            "expected": expected,
            "actual": actual,
            "match": match,
        })
        if not match:
            console.print(
                f"  [red]MISMATCH VM {pattern_type}[{pattern_idx}] v{version}: "
                f"expected={expected} actual={actual}"
            )

    return results


def verify_pattern_vq(
    pattern_idx: int,
    pattern: Tuple[str, str, str],
    pattern_type: str,
    config: dict,
) -> List[dict]:
    result_prefix = f"ver-{pattern_type}-queries"
    result_dir = RESULTS_DIR / pattern_type / "ver" / result_prefix
    file_idx = pattern_idx + 1
    result_file = result_dir / f"{result_prefix}-{file_idx}.txt"

    if not result_file.exists():
        console.print(f"  [yellow]Result file not found: {result_file}")
        return []

    expected_counts = parse_bear_result_file(result_file)
    sparql = bear_pattern_to_sparql(pattern, pattern_type)
    actual_by_ts = run_vq_query(sparql, config)

    actual_by_version = {}
    for ts, count in actual_by_ts.items():
        ts_dt = datetime.fromisoformat(ts)
        version = int((ts_dt - BASE_TIMESTAMP) / INTERVAL)
        actual_by_version[version] = count

    results = []
    all_versions = set(expected_counts.keys()) | set(actual_by_version.keys())
    mismatches = 0
    for version in sorted(all_versions):
        if version >= NUM_VERSIONS:
            continue
        expected = expected_counts.get(version, 0)
        actual = actual_by_version.get(version, 0)
        match = expected == actual
        results.append({
            "query_type": "vq",
            "pattern_type": pattern_type,
            "pattern_index": pattern_idx,
            "version": version,
            "expected": expected,
            "actual": actual,
            "match": match,
        })
        if not match:
            mismatches += 1

    if mismatches > 0:
        console.print(
            f"  [red]MISMATCH VQ {pattern_type}[{pattern_idx}]: "
            f"{mismatches} versions differ"
        )

    return results


def print_summary(results: List[dict]) -> None:
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


def main():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)

    all_patterns = []
    for pattern_type in ["p", "po"]:
        query_file = QUERIES_DIR / f"{pattern_type}.txt"
        if not query_file.exists():
            console.print(f"[yellow]Query file not found: {query_file}")
            continue
        patterns = parse_bear_query_file(query_file)
        for idx, pattern in enumerate(patterns):
            all_patterns.append((idx, pattern, pattern_type))

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

        for pattern_idx, pattern, pattern_type in all_patterns:
            status_text = f"[green]{passed}[/green] OK  [red]{failed}[/red] FAIL"
            progress.update(task, label=f"{pattern_type}[{pattern_idx}] VM", status=status_text)
            vm_results = verify_pattern_vm(pattern_idx, pattern, pattern_type, config)
            vm_ok = all(r["match"] for r in vm_results) if vm_results else True
            all_results.extend(vm_results)
            progress.advance(task)

            progress.update(task, label=f"{pattern_type}[{pattern_idx}] VQ")
            vq_results = verify_pattern_vq(pattern_idx, pattern, pattern_type, config)
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
    print_summary(all_results)

    output_file = DATA_DIR / "verification_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)
    console.print(f"\nDetailed results saved to {output_file}")


if __name__ == "__main__":
    main()
