# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import json
import re
import statistics
import subprocess
from pathlib import Path

import corpora
from protocol import (
    DEFAULT_REPLICATIONS,
    build_manifest,
    docker_image_id,
    git_dirty,
    git_revision,
    hardware_info,
    manifest_patterns,
    protocol_metadata,
)
from rich.console import Console
from rich.table import Table

console = Console()

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
OSTRICH_DIR = DATA_DIR / "ostrich"
QUERIES_DIR = OSTRICH_DIR / "queries"
IMAGE_NAME = "ostrich-bear"

DEFAULT_GROUP_TIMEOUT_S = 3600


def write_query_file(query_file: Path, patterns: list[dict]) -> None:
    query_file.parent.mkdir(parents=True, exist_ok=True)
    contents = "".join(f"{' '.join(pattern['triple'])} .\n" for pattern in patterns)
    query_file.write_text(contents, encoding="utf-8")


def run_ostrich_queries(
    query_file: Path,
    evalrun_dir: Path,
    replications: int,
    timeout_s: int,
) -> str:
    cmd = [
        "docker",
        "run",
        "--rm",
        "--ulimit",
        "nofile=65536:65536",
        "-v",
        f"{evalrun_dir}:/var/evalrun",
        "-v",
        f"{query_file.parent}:/var/queries",
        IMAGE_NAME,
        "query",
        f"/var/queries/{query_file.name}",
        str(replications),
    ]
    console.print(f"Running OSTRICH queries for {query_file.name}...")
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout_s, check=False
    )
    if result.returncode != 0:
        console.print(f"[red]OSTRICH error: {result.stderr[:500]}")
        msg = f"OSTRICH query failed with exit code {result.returncode}"
        raise RuntimeError(msg)
    return result.stdout


def parse_ostrich_output(raw_output: str) -> list[dict]:
    patterns = []
    current_pattern = None
    current_section = None

    for raw_line in raw_output.splitlines():
        line = raw_line.strip()

        match = re.match(r"---PATTERN START:\s*(.+)", line)
        if match:
            current_pattern = {
                "triple_pattern": match.group(1).strip(),
                "vm": [],
                "dm": [],
                "vq": [],
            }
            patterns.append(current_pattern)
            current_section = None
            continue

        if "---VERSION MATERIALIZED" in line:
            current_section = "vm"
            continue
        if "---DELTA MATERIALIZED" in line:
            current_section = "dm"
            continue
        if line.startswith("--- ---VERSION"):
            current_section = "vq"
            continue

        if line.startswith(("---", "patch,", "patch_start,", "offset,")):
            continue

        if current_pattern is None or current_section is None:
            continue

        parts = line.split(",")
        if current_section == "vm" and len(parts) >= 7:
            current_pattern["vm"].append(
                {
                    "patch": int(parts[0]),
                    "median_us": float(parts[4]),
                    "lookup_us": float(parts[5]),
                    "results": int(parts[6]),
                }
            )
        elif current_section == "dm" and len(parts) >= 8:
            current_pattern["dm"].append(
                {
                    "patch_start": int(parts[0]),
                    "patch_end": int(parts[1]),
                    "median_us": float(parts[5]),
                    "lookup_us": float(parts[6]),
                    "results": int(parts[7]),
                }
            )
        elif current_section == "vq" and len(parts) >= 5:
            current_pattern["vq"].append(
                {
                    "median_us": float(parts[2]),
                    "lookup_us": float(parts[3]),
                    "results": int(parts[4]),
                }
            )

    return patterns


def align_patterns(parsed_patterns: list[dict], selected: list[dict]) -> None:
    if len(parsed_patterns) != len(selected):
        msg = (
            f"OSTRICH returned {len(parsed_patterns)} patterns for "
            f"{len(selected)} manifest entries"
        )
        raise RuntimeError(msg)
    for parsed, manifest_entry in zip(parsed_patterns, selected, strict=True):
        parsed["pattern_index"] = manifest_entry["pattern_index"]


def filter_dm_workload(patterns: list[dict], corpus: corpora.Corpus) -> None:
    endpoints = set(range(corpus.dm_step, corpus.num_versions, corpus.dm_step))
    endpoints.add(corpus.num_versions - 1)
    for pattern in patterns:
        pattern["dm"] = [
            entry
            for entry in pattern["dm"]
            if entry["patch_start"] == 0 and entry["patch_end"] in endpoints
        ]


def build_per_version_detail(all_patterns: dict[str, list[dict]]) -> dict:
    vm_by_version: dict[int, list] = {}
    dm_by_delta: dict[tuple[int, int], list] = {}
    vq_entries: list[dict] = []

    for pattern_type, patterns in all_patterns.items():
        for pattern in patterns:
            pattern_index = pattern["pattern_index"]
            for entry in pattern["vm"]:
                version = entry["patch"]
                vm_by_version.setdefault(version, []).append(
                    {
                        "pattern_type": pattern_type,
                        "pattern_index": pattern_index,
                        "median_us": entry["median_us"],
                        "results": entry["results"],
                    }
                )
            for entry in pattern["dm"]:
                key = (entry["patch_start"], entry["patch_end"])
                dm_by_delta.setdefault(key, []).append(
                    {
                        "pattern_type": pattern_type,
                        "pattern_index": pattern_index,
                        "median_us": entry["median_us"],
                        "results": entry["results"],
                    }
                )
            vq_entries.extend(
                {
                    "pattern_type": pattern_type,
                    "pattern_index": pattern_index,
                    "median_us": entry["median_us"],
                    "results": entry["results"],
                }
                for entry in pattern["vq"]
            )

    per_version_vm = [
        {"version": v, "patterns": pats} for v, pats in sorted(vm_by_version.items())
    ]
    per_delta_dm = [
        {"version_start": k[0], "version_end": k[1], "patterns": pats}
        for k, pats in sorted(dm_by_delta.items())
    ]

    return {
        "per_version_vm": per_version_vm,
        "per_delta_dm": per_delta_dm,
        "per_pattern_vq": vq_entries,
    }


def aggregate_results(all_patterns: dict[str, list[dict]]) -> dict:
    results = {}

    for query_type in ["vm", "dm", "vq"]:
        all_medians_us = []
        by_pattern_type = {}

        for pattern_type, patterns in all_patterns.items():
            pattern_medians = []
            for pattern in patterns:
                entries = pattern[query_type]
                pattern_medians.extend(entry["median_us"] for entry in entries)
            if pattern_medians:
                all_medians_us.extend(pattern_medians)
                by_pattern_type[pattern_type] = {
                    "count": len(pattern_medians),
                    "mean_us": statistics.mean(pattern_medians),
                    "median_us": statistics.median(pattern_medians),
                    "mean_ms": statistics.mean(pattern_medians) / 1000,
                    "median_ms": statistics.median(pattern_medians) / 1000,
                }

        if all_medians_us:
            results[query_type] = {
                "count": len(all_medians_us),
                "mean_us": statistics.mean(all_medians_us),
                "median_us": statistics.median(all_medians_us),
                "mean_ms": statistics.mean(all_medians_us) / 1000,
                "median_ms": statistics.median(all_medians_us) / 1000,
                "by_pattern": by_pattern_type,
            }

    return results


def print_summary(results: dict, corpus_name: str) -> None:
    table = Table(title=f"OSTRICH benchmark results ({corpus_name})")
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


def parse_ingestion_time(ingestion_log: Path) -> float | None:
    if not ingestion_log.exists():
        return None
    total_ms = 0.0
    found = False
    for line in ingestion_log.read_text().splitlines():
        parts = line.strip().split(",")
        if len(parts) >= 3 and parts[0].isdigit():
            total_ms += float(parts[2])
            found = True
    return total_ms / 1000 if found else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--corpus", choices=corpora.CORPUS_NAMES, default="bear-b-daily"
    )
    parser.add_argument("--replications", type=int, default=DEFAULT_REPLICATIONS)
    parser.add_argument("--timeout", type=int, default=DEFAULT_GROUP_TIMEOUT_S)
    args = parser.parse_args()
    if args.replications < 1:
        parser.error("--replications must be at least 1")
    if args.timeout < 1:
        parser.error("--timeout must be at least 1")

    corpus = corpora.get(args.corpus)
    manifest = build_manifest(corpus)
    protocol = protocol_metadata(
        manifest,
        args.replications,
        measurement="time",
        sparql_request_timeout_s=args.timeout,
    )
    protocol["system"] = "ostrich"
    protocol["engine_image_id"] = docker_image_id(IMAGE_NAME)
    protocol["engine_revision"] = git_revision(OSTRICH_DIR / "ostrich-repo")
    protocol["engine_dirty"] = git_dirty(OSTRICH_DIR / "ostrich-repo")
    protocol["timeout_scope"] = "query group"
    protocol["warmup"] = "native untimed executions per case"
    evalrun_dir = OSTRICH_DIR / f"evalrun_{corpus.name}"
    ingestion_log = OSTRICH_DIR / f"ingestion_output_{corpus.name}.txt"
    output_file = DATA_DIR / f"ostrich_benchmark_results_{corpus.name}.json"
    query_dir = QUERIES_DIR / corpus.name / protocol["manifest_hash"]

    all_patterns: dict[str, list[dict]] = {}

    for query_set in corpus.queries:
        pattern_type = query_set.name
        patterns_in_manifest = manifest_patterns(manifest, pattern_type)
        query_file = query_dir / f"{pattern_type}.txt"
        write_query_file(query_file, patterns_in_manifest)
        raw_output = run_ostrich_queries(
            query_file,
            evalrun_dir,
            args.replications,
            args.timeout,
        )

        raw_path = DATA_DIR / f"ostrich_raw_{pattern_type}_{corpus.name}.txt"
        with raw_path.open("w", encoding="utf-8") as f:
            f.write(raw_output)
        console.print(f"  Raw output saved to {raw_path}")

        patterns = parse_ostrich_output(raw_output)
        align_patterns(patterns, patterns_in_manifest)
        filter_dm_workload(patterns, corpus)
        all_patterns[pattern_type] = patterns
        console.print(f"  Parsed {len(patterns)} patterns from {query_file.name}")

    results = aggregate_results(all_patterns)
    detail = build_per_version_detail(all_patterns)

    ingestion_s = parse_ingestion_time(ingestion_log)
    if ingestion_s is not None:
        console.print(f"OSTRICH ingestion time: {ingestion_s:.2f}s")

    output = {
        "hardware": hardware_info(),
        "protocol": protocol,
        "replications": args.replications,
        "ingestion_s": ingestion_s,
        "results": results,
        "detail": detail,
        "raw_patterns": {
            pt: [p["triple_pattern"] for p in pats] for pt, pats in all_patterns.items()
        },
    }

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    console.print(f"\nResults saved to {output_file}")

    print_summary(results, corpus.name)


if __name__ == "__main__":
    main()
