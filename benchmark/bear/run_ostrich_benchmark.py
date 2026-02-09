import argparse
import json
import re
import statistics
import subprocess
from pathlib import Path

from rich.console import Console
from rich.table import Table

console = Console()

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
OSTRICH_DIR = DATA_DIR / "ostrich"
QUERIES_DIR = OSTRICH_DIR / "queries"
IMAGE_NAME = "ostrich-bear"

NUM_REPLICATIONS = 5
QUERY_FILES = ["p.txt", "po.txt"]


def run_ostrich_queries(query_file: str, evalrun_dir: Path) -> str:
    cmd = [
        "docker", "run", "--rm",
        "--ulimit", "nofile=65536:65536",
        "-v", f"{evalrun_dir}:/var/evalrun",
        "-v", f"{QUERIES_DIR}:/var/queries",
        IMAGE_NAME,
        "query",
        f"/var/queries/{query_file}", str(NUM_REPLICATIONS),
    ]
    console.print(f"Running OSTRICH queries for {query_file}...")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    if result.returncode != 0:
        console.print(f"[red]OSTRICH error: {result.stderr[:500]}")
        raise RuntimeError(f"OSTRICH query failed with exit code {result.returncode}")
    return result.stdout


def parse_ostrich_output(raw_output: str) -> list[dict]:
    patterns = []
    current_pattern = None
    current_section = None

    for line in raw_output.splitlines():
        line = line.strip()

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

        if line.startswith("---") or line.startswith("patch,") or line.startswith("patch_start,") or line.startswith("offset,"):
            continue

        if current_pattern is None or current_section is None:
            continue

        parts = line.split(",")
        if current_section == "vm" and len(parts) >= 7:
            current_pattern["vm"].append({
                "patch": int(parts[0]),
                "median_us": float(parts[4]),
                "lookup_us": float(parts[5]),
                "results": int(parts[6]),
            })
        elif current_section == "dm" and len(parts) >= 8:
            current_pattern["dm"].append({
                "patch_start": int(parts[0]),
                "patch_end": int(parts[1]),
                "median_us": float(parts[5]),
                "lookup_us": float(parts[6]),
                "results": int(parts[7]),
            })
        elif current_section == "vq" and len(parts) >= 5:
            current_pattern["vq"].append({
                "median_us": float(parts[2]),
                "lookup_us": float(parts[3]),
                "results": int(parts[4]),
            })

    return patterns


def build_per_version_detail(all_patterns: dict[str, list[dict]]) -> dict:
    vm_by_version: dict[int, list] = {}
    dm_by_delta: dict[tuple[int, int], list] = {}
    vq_entries: list[dict] = []

    for pattern_type, patterns in all_patterns.items():
        for pat_idx, pattern in enumerate(patterns):
            for entry in pattern["vm"]:
                version = entry["patch"]
                vm_by_version.setdefault(version, []).append({
                    "pattern_type": pattern_type,
                    "pattern_index": pat_idx,
                    "median_us": entry["median_us"],
                })
            for entry in pattern["dm"]:
                key = (entry["patch_start"], entry["patch_end"])
                dm_by_delta.setdefault(key, []).append({
                    "pattern_type": pattern_type,
                    "pattern_index": pat_idx,
                    "median_us": entry["median_us"],
                })
            for entry in pattern["vq"]:
                vq_entries.append({
                    "pattern_type": pattern_type,
                    "pattern_index": pat_idx,
                    "median_us": entry["median_us"],
                })

    per_version_vm = [
        {"version": v, "patterns": pats}
        for v, pats in sorted(vm_by_version.items())
    ]
    per_delta_dm = [
        {"patch_start": k[0], "patch_end": k[1], "patterns": pats}
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
                if entries:
                    medians = [e["median_us"] for e in entries]
                    avg_median = statistics.mean(medians)
                    pattern_medians.append(avg_median)
                    all_medians_us.append(avg_median)
            if pattern_medians:
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


def print_summary(results: dict) -> None:
    table = Table(title="OSTRICH benchmark results (BEAR-B-daily)")
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
    parser.add_argument("--granularity", choices=["daily", "hourly", "instant"], default="daily")
    args = parser.parse_args()

    evalrun_dir = OSTRICH_DIR / f"evalrun_{args.granularity}"
    ingestion_log = OSTRICH_DIR / f"ingestion_output_{args.granularity}.txt"
    output_file = DATA_DIR / f"ostrich_benchmark_results_{args.granularity}.json"

    all_patterns: dict[str, list[dict]] = {}

    for query_file in QUERY_FILES:
        pattern_type = query_file.replace(".txt", "")
        raw_output = run_ostrich_queries(query_file, evalrun_dir)

        raw_path = DATA_DIR / f"ostrich_raw_{pattern_type}.txt"
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(raw_output)
        console.print(f"  Raw output saved to {raw_path}")

        patterns = parse_ostrich_output(raw_output)
        all_patterns[pattern_type] = patterns
        console.print(f"  Parsed {len(patterns)} patterns from {query_file}")

    results = aggregate_results(all_patterns)
    detail = build_per_version_detail(all_patterns)

    ingestion_s = parse_ingestion_time(ingestion_log)
    if ingestion_s is not None:
        console.print(f"OSTRICH ingestion time: {ingestion_s:.2f}s")

    output = {
        "replications": NUM_REPLICATIONS,
        "ingestion_s": ingestion_s,
        "results": results,
        "detail": detail,
        "raw_patterns": {pt: [p["triple_pattern"] for p in pats] for pt, pats in all_patterns.items()},
    }

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    console.print(f"\nResults saved to {output_file}")

    print_summary(results)


if __name__ == "__main__":
    main()
