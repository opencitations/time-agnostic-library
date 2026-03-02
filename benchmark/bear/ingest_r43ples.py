import argparse
import gzip
import json
import time
from pathlib import Path

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

console = Console()

GRAPH_URI = "http://bear.benchmark/dataset"
BATCH_SIZE = 5000
NUM_VERSIONS_MAP = {"daily": 89, "hourly": 1299, "instant": 21046}

PROGRESS_COLUMNS = (
    SpinnerColumn(),
    TextColumn("[bold blue]{task.description}"),
    BarColumn(),
    MofNCompleteColumn(),
    TimeElapsedColumn(),
    TimeRemainingColumn(),
)


def read_safe_triples(filepath: Path) -> list[str]:
    safe = []
    with gzip.open(filepath, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split('"')
            if len(parts) > 3:
                continue
            if '\\n' in line or '\\t' in line:
                continue
            # R43ples' getStringEnclosedInBraces counts {/} without skipping
            # string literals, so braces inside literals corrupt query parsing
            if len(parts) >= 2 and ('{' in parts[1] or '}' in parts[1]):
                continue
            safe.append(line)
    return safe


def send_update(endpoint: str, action: str, triples: list[str]) -> bool:
    body = "\n".join(triples)
    query = (
        f'USER "benchmark" MESSAGE "v" '
        f'{action} DATA {{ GRAPH <{GRAPH_URI}> BRANCH "master" {{ {body} }} }}'
    )
    resp = requests.post(
        endpoint,
        data={"query": query},
        headers={"Accept": "text/plain"},
        timeout=300,
    )
    if resp.text.strip() == "Update executed":
        return True
    console.print(f"[yellow]Response: {resp.text.strip()[:100]}")
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--granularity", choices=["daily", "hourly", "instant"], default="daily")
    parser.add_argument("--port", type=int, default=9998)
    args = parser.parse_args()

    num_versions = NUM_VERSIONS_MAP[args.granularity]
    endpoint = f"http://localhost:{args.port}/r43ples/sparql"
    data_dir = Path(__file__).parent / "data"
    ic_dir = data_dir / args.granularity / "IC"
    cb_dir = data_dir / args.granularity / "CB"

    console.print(f"[bold]R43ples ingestion ({args.granularity}, {num_versions} versions)")

    requests.post(
        endpoint,
        data={"query": f"CREATE SILENT GRAPH <{GRAPH_URI}>"},
        headers={"Accept": "text/plain"},
        timeout=60,
    )

    start_time = time.perf_counter()

    # Load initial snapshot
    triples = read_safe_triples(ic_dir / "000001.nt.gz")
    num_batches = (len(triples) + BATCH_SIZE - 1) // BATCH_SIZE
    console.print(f"  Initial snapshot: {len(triples)} triples ({num_batches} batches)")
    for i in range(0, len(triples), BATCH_SIZE):
        send_update(endpoint, "INSERT", triples[i:i + BATCH_SIZE])

    revision_map: dict[int, int] = {1: num_batches}
    current_rev = num_batches

    # Apply CB changes
    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task("CB changes", total=num_versions - 1)
        for v in range(2, num_versions + 1):
            prev = v - 1
            for action, name in [("DELETE", "deleted"), ("INSERT", "added")]:
                f = cb_dir / f"data-{name}_{prev}-{v}.nt.gz"
                if f.exists():
                    safe = read_safe_triples(f)
                    if safe and send_update(endpoint, action, safe):
                        current_rev += 1
            revision_map[v] = current_rev
            progress.advance(task)

    elapsed = time.perf_counter() - start_time
    console.print(f"\n[bold green]Ingestion complete in {elapsed:.2f}s")
    console.print(f"  R43ples revisions: {current_rev}")

    map_file = data_dir / f"r43ples_revision_map_{args.granularity}.json"
    with open(map_file, "w", encoding="utf-8") as f:
        json.dump(revision_map, f)
    console.print(f"  Revision map: {map_file}")


if __name__ == "__main__":
    main()
