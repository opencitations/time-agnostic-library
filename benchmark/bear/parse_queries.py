from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

from rich.console import Console

console = Console()

GRANULARITY_CONFIG = {
    "daily": {"num_versions": 89, "interval": timedelta(days=1)},
    "hourly": {"num_versions": 1299, "interval": timedelta(hours=1)},
    "instant": {"num_versions": 21046, "interval": timedelta(minutes=1)},
}

GRANULARITY_PORTS = {"daily": 7001, "hourly": 7002, "instant": 7003}


def build_config(granularity: str) -> dict:
    port = GRANULARITY_PORTS[granularity]
    url = f"http://localhost:{port}"
    return {
        "dataset": {
            "triplestore_urls": [url],
            "file_paths": [],
            "is_quadstore": True,
        },
        "provenance": {
            "triplestore_urls": [url],
            "file_paths": [],
            "is_quadstore": True,
        },
        "blazegraph_full_text_search": "no",
        "fuseki_full_text_search": "no",
        "virtuoso_full_text_search": "no",
        "graphdb_connector_name": "",
    }

BASE_TIMESTAMP = datetime.fromisoformat("2015-08-01T00:00:00+00:00")

DATA_DIR = Path(__file__).parent / "data"
QUERIES_DIR = DATA_DIR / "queries"


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


def generate_timestamps(num_versions: int, interval: timedelta) -> List[str]:
    return [(BASE_TIMESTAMP + interval * i).strftime("%Y-%m-%dT%H:%M:%S+00:00") for i in range(num_versions)]


def generate_vm_queries(
    patterns: List[Tuple[str, str, str]],
    pattern_type: str,
    timestamps: List[str],
) -> List[dict]:
    queries = []
    for i, ts in enumerate(timestamps):
        for j, pattern in enumerate(patterns):
            sparql = bear_pattern_to_sparql(pattern, pattern_type)
            queries.append({
                "type": "vm",
                "pattern_type": pattern_type,
                "pattern_index": j,
                "version_index": i,
                "timestamp": ts,
                "sparql": sparql,
                "on_time": (ts, ts),
            })
    return queries


def generate_dm_queries(
    patterns: List[Tuple[str, str, str]],
    pattern_type: str,
    timestamps: List[str],
    dm_step: int,
) -> List[dict]:
    diff_versions = list(range(dm_step, min(len(timestamps), dm_step * 11 + 1), dm_step))
    if len(timestamps) - 1 not in diff_versions:
        diff_versions.append(len(timestamps) - 1)

    queries = []
    t0 = timestamps[0]
    for vi in diff_versions:
        if vi >= len(timestamps):
            continue
        ti = timestamps[vi]
        for j, pattern in enumerate(patterns):
            sparql = bear_pattern_to_sparql(pattern, pattern_type)
            queries.append({
                "type": "dm",
                "pattern_type": pattern_type,
                "pattern_index": j,
                "version_start": 0,
                "version_end": vi,
                "timestamp_start": t0,
                "timestamp_end": ti,
                "sparql": sparql,
                "on_time": (t0, ti),
            })
    return queries


def generate_vq_queries(
    patterns: List[Tuple[str, str, str]],
    pattern_type: str,
) -> List[dict]:
    queries = []
    for j, pattern in enumerate(patterns):
        sparql = bear_pattern_to_sparql(pattern, pattern_type)
        queries.append({
            "type": "vq",
            "pattern_type": pattern_type,
            "pattern_index": j,
            "sparql": sparql,
            "on_time": None,
        })
    return queries


def parse_and_generate(num_versions: int, interval: timedelta, dm_step: int) -> dict:
    timestamps = generate_timestamps(num_versions, interval)
    all_queries: dict[str, list] = {"vm": [], "dm": [], "vq": []}

    for pattern_type in ["p", "po"]:
        query_file = QUERIES_DIR / f"{pattern_type}.txt"
        if not query_file.exists():
            console.print(f"  [yellow]Query file not found: {query_file}")
            continue
        patterns = parse_bear_query_file(query_file)
        console.print(f"  Parsed {len(patterns)} {pattern_type} patterns")

        all_queries["vm"].extend(generate_vm_queries(patterns, pattern_type, timestamps))
        all_queries["dm"].extend(generate_dm_queries(patterns, pattern_type, timestamps, dm_step))
        all_queries["vq"].extend(generate_vq_queries(patterns, pattern_type))

    return all_queries


DM_STEPS = {"daily": 5, "hourly": 100, "instant": 1500}
