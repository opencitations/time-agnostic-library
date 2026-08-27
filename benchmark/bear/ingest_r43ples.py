# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import json
import time
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass
from pathlib import Path

import corpora
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

PROGRESS_COLUMNS = (
    SpinnerColumn(),
    TextColumn("[bold blue]{task.description}"),
    BarColumn(),
    MofNCompleteColumn(),
    TimeElapsedColumn(),
    TimeRemainingColumn(),
)


@dataclass
class IngestionStats:
    accepted: int = 0
    unsafe_quotes: int = 0
    escaped_whitespace: int = 0
    literal_braces: int = 0

    @property
    def skipped(self) -> int:
        return self.unsafe_quotes + self.escaped_whitespace + self.literal_braces


def read_safe_triples(filepath: Path, stats: IngestionStats) -> Iterator[str]:
    with filepath.open(encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line:
                continue
            parts = line.split('"')
            if len(parts) > 3:
                stats.unsafe_quotes += 1
                continue
            if "\\n" in line or "\\t" in line:
                stats.escaped_whitespace += 1
                continue
            if len(parts) >= 2 and ("{" in parts[1] or "}" in parts[1]):
                stats.literal_braces += 1
                continue
            stats.accepted += 1
            yield line


def triple_batches(
    triples: Iterable[str], batch_size: int = BATCH_SIZE
) -> Iterator[list[str]]:
    batch = []
    for triple in triples:
        batch.append(triple)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def send_update(endpoint: str, action: str, triples: list[str]) -> None:
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
        return
    msg = f"R43ples rejected {action}: {resp.text.strip()[:100]}"
    raise RuntimeError(msg)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--corpus", choices=corpora.CORPUS_NAMES, default="bear-b-daily"
    )
    parser.add_argument("--port", type=int, default=9998)
    args = parser.parse_args()

    corpus = corpora.get(args.corpus)
    num_versions = corpus.num_versions
    endpoint = f"http://localhost:{args.port}/r43ples/sparql"
    ic_dir = corpus.dir / "IC"
    cb_dir = corpus.dir / "CB"

    console.print(f"[bold]R43ples ingestion ({corpus.name}, {num_versions} versions)")

    requests.post(
        endpoint,
        data={"query": f"CREATE SILENT GRAPH <{GRAPH_URI}>"},
        headers={"Accept": "text/plain"},
        timeout=60,
    )

    start_time = time.perf_counter()
    stats = IngestionStats()

    # Load initial snapshot
    initial_snapshot = min(ic_dir.glob("*.nt"), key=lambda path: int(path.stem))
    num_batches = 0
    num_triples = 0
    for triples in triple_batches(read_safe_triples(initial_snapshot, stats)):
        send_update(endpoint, "INSERT", triples)
        num_batches += 1
        num_triples += len(triples)
    console.print(f"  Initial snapshot: {num_triples} triples ({num_batches} batches)")

    revision_map: dict[int, int] = {1: num_batches}
    current_rev = num_batches

    # Apply CB changes
    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task("CB changes", total=num_versions - 1)
        for v in range(2, num_versions + 1):
            prev = v - 1
            for action, name in [("DELETE", "deleted"), ("INSERT", "added")]:
                f = cb_dir / f"data-{name}_{prev}-{v}.nt"
                if f.exists():
                    for triples in triple_batches(read_safe_triples(f, stats)):
                        send_update(endpoint, action, triples)
                        current_rev += 1
            revision_map[v] = current_rev
            progress.advance(task)

    elapsed = time.perf_counter() - start_time
    console.print(f"\n[bold green]Ingestion complete in {elapsed:.2f}s")
    console.print(f"  R43ples revisions: {current_rev}")

    map_file = corpora.DATA_DIR / f"r43ples_revision_map_{corpus.name}.json"
    with map_file.open("w", encoding="utf-8") as f:
        json.dump(revision_map, f)
    console.print(f"  Revision map: {map_file}")

    stats_file = corpora.DATA_DIR / f"r43ples_ingestion_stats_{corpus.name}.json"
    with stats_file.open("w", encoding="utf-8") as file:
        json.dump({**asdict(stats), "skipped": stats.skipped}, file, indent=2)
    console.print(f"  Accepted triple occurrences: {stats.accepted}")
    console.print(f"  Skipped triple occurrences: {stats.skipped}")
    console.print(f"  Ingestion statistics: {stats_file}")


if __name__ == "__main__":
    main()
