# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import gzip
import json
import os
import shutil
import subprocess
import sys
import time
from collections import deque
from collections.abc import Iterator
from concurrent.futures import Future, ProcessPoolExecutor
from importlib.metadata import version
from pathlib import Path
from typing import IO, cast

from rdflib.plugins.parsers.ntriples import unquote
from rich.console import Console
from sparqlite import SPARQLClient

from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.qlever import QLEVER_HAS_WORD, generate_qlever_index

sys.path.insert(0, str(Path(__file__).parent))
import corpora
from convert_to_ocdm import XSD_NS
from protocol import DEFAULT_TIMEOUT_S, docker_image_id

console = Console()

QLEVER_IMAGE = "docker.io/adfreiburg/qlever"
# Same graph as time_agnostic_library.qlever_setup.
ASSOCIATION_GRAPH = "<urn:tal:qlever:prov/>"
UPDATE_QUERY_PREDICATE = f"<{ProvEntity.iri_has_update_query}> "
INPUT_NAMES = ("dataset.nq.gz", "provenance.nq.gz", "associations.nq.gz")
ASSOCIATION_RECORD = "associations.json"
STRIP_SCRIPTS = ("strip_dataset.sed", "strip_provenance.sed")
LOADER_FILES = {*INPUT_NAMES, ASSOCIATION_RECORD, *STRIP_SCRIPTS}
# The longest update query of BEAR-A spans 46 MB, above the 10 MB default.
PARSER_BUFFER_SIZE = "100M"
BATCH_BYTES = 8 * 1024 * 1024
# Index and server sizing from the Qleverfile.wikidata shipped with the CLI and
# from the previous QLever setup of this benchmark.
INDEX_SETTINGS = {"prefixes-external": [], "num-triples-per-batch": 5_000_000}
STXXL_MEMORY = "10G"
MEMORY_FOR_QUERIES = "16G"
CACHE_MAX_SIZE = "8G"


def _snapshot_updates(update_lines: bytes) -> Iterator[tuple[str, str]]:
    # The lines hold a plain literal after the snapshot and the predicate, so
    # the last quote closes the update query. This avoids an RDF parser, which
    # rejects the IRIs of BEAR that break RFC 3987 and the update queries above
    # 16 MiB, both of which QLever accepts.
    for line in update_lines.decode().splitlines():
        snapshot_end = line.index("> ")
        if not line.startswith(UPDATE_QUERY_PREDICATE, snapshot_end + 2):
            continue
        literal_start = line.index('"', snapshot_end) + 1
        yield line[1:snapshot_end], unquote(line[literal_start : line.rindex('"')])


def association_quads(update_lines: bytes) -> bytes:
    snapshots = _snapshot_updates(update_lines)
    associations = "".join(
        f"{snapshot.n3()} {predicate.n3()} {uri.n3()} {ASSOCIATION_GRAPH} .\n"
        for snapshot, predicate, uri in generate_qlever_index(snapshots)
    )
    # Each batch becomes one gzip member, so the workers compress in parallel
    # and the concatenated output is a valid gzip file.
    return gzip.compress(associations.encode(), compresslevel=1)


def _batches(stream: IO[bytes], batch_bytes: int) -> Iterator[bytes]:
    while chunk := stream.read(batch_bytes):
        yield chunk + stream.readline()


def write_associations(
    provenance: Path, output: Path, batch_bytes: int = BATCH_BYTES
) -> None:
    # grep keeps only the update-query lines of the streamed provenance, so
    # Python parses a fraction of the corpus. Every line is independent, hence
    # the batches go to separate processes.
    command = [
        "bash",
        "-o",
        "pipefail",
        "-c",
        'zcat "$1" | grep -F "$2"',
        "_",
        str(provenance),
        UPDATE_QUERY_PREDICATE,
    ]
    workers = os.cpu_count() or 1
    pending: deque[Future[bytes]] = deque()
    with (
        subprocess.Popen(command, stdout=subprocess.PIPE) as update_lines,
        output.open("wb") as out,
        ProcessPoolExecutor(workers) as pool,
    ):
        stream = cast("IO[bytes]", update_lines.stdout)
        for batch in _batches(stream, batch_bytes):
            pending.append(pool.submit(association_quads, batch))
            if len(pending) > 2 * workers:
                out.write(pending.popleft().result())
        while pending:
            out.write(pending.popleft().result())
    if update_lines.returncode:
        raise subprocess.CalledProcessError(update_lines.returncode, command)


def write_strip_scripts(index_dir: Path) -> None:
    # QLever rewrites the literals of the datatypes it understands, such as
    # "647500"^^xsd:double into "647500.0"^^xsd:decimal, and rejects malformed
    # ones. The library compares stored quads and update queries as strings,
    # and the published answers keep the original lexical forms, so the loader
    # drops the XSD datatypes of the dataset and of the update queries. The
    # provenance timestamps keep theirs, because the quotes of a literal inside
    # an update query are escaped.
    datatype = "\\^\\^<" + XSD_NS.replace(".", "\\.") + "[A-Za-z]+>"
    (index_dir / STRIP_SCRIPTS[0]).write_text(
        's|"' + datatype + '|"|g\n', encoding="utf-8"
    )
    (index_dir / STRIP_SCRIPTS[1]).write_text(
        's|\\\\"' + datatype + '|\\\\"|g\n', encoding="utf-8"
    )


def build_index(name: str, index_dir: Path, container: str) -> None:
    write_strip_scripts(index_dir)
    inputs = [
        {
            "cmd": f"zcat {INPUT_NAMES[0]} | sed -E -f {STRIP_SCRIPTS[0]}",
            "format": "nq",
            "parallel": "true",
        },
        {
            "cmd": f"zcat {INPUT_NAMES[1]} | sed -E -f {STRIP_SCRIPTS[1]}",
            "format": "nq",
            "parallel": "true",
        },
        {"cmd": f"zcat {INPUT_NAMES[2]}", "format": "nq", "parallel": "true"},
    ]
    subprocess.run(
        [
            "qlever",
            "index",
            "--name",
            name,
            "--format",
            "nq",
            "--input-files",
            " ".join(INPUT_NAMES),
            "--multi-input-json",
            json.dumps(inputs),
            "--settings-json",
            json.dumps(INDEX_SETTINGS),
            "--stxxl-memory",
            STXXL_MEMORY,
            "--parser-buffer-size",
            PARSER_BUFFER_SIZE,
            "--index-container",
            f"{container}-index",
            "--system",
            "docker",
            "--overwrite-existing",
        ],
        cwd=index_dir,
        check=True,
    )


def store_bytes(index_dir: Path) -> int:
    return sum(
        path.stat().st_size
        for path in index_dir.iterdir()
        if path.name not in LOADER_FILES
    )


def start_server(corpus: corpora.Corpus, index_dir: Path, container: str) -> None:
    # The CLI option --kill-existing-with-same-port matches every process whose
    # command line carries the port, including QLever servers of other Docker
    # networks, so only the container of this corpus is replaced.
    subprocess.run(["docker", "rm", "-f", container], check=True, capture_output=True)
    subprocess.run(
        [
            "qlever",
            "start",
            "--name",
            corpus.name,
            "--description",
            f"TAL {corpus.name}",
            "--host-name",
            "localhost",
            "--port",
            str(corpus.port),
            "--server-container",
            container,
            "--access-token",
            "",
            "--memory-for-queries",
            MEMORY_FOR_QUERIES,
            "--cache-max-size",
            CACHE_MAX_SIZE,
            "--timeout",
            f"{DEFAULT_TIMEOUT_S}s",
            "--no-warmup",
            "--system",
            "docker",
        ],
        cwd=index_dir,
        check=True,
    )
    with SPARQLClient(corpus.endpoint()) as client:
        if not client.ask(f"ASK {{ ?snapshot <{QLEVER_HAS_WORD}> ?uri }}"):
            msg = "The QLever index contains no URI association."
            raise RuntimeError(msg)


def extract_associations(corpus: corpora.Corpus, index_dir: Path) -> None:
    dataset = corpus.dir / INPUT_NAMES[0]
    provenance = corpus.dir / INPUT_NAMES[1]
    for required in (dataset, provenance):
        if not required.is_file():
            console.print(
                f"[red]{required} not found. "
                f"Run convert_to_ocdm.py --corpus {corpus.name} first."
            )
            sys.exit(1)
    # Hard links keep the inputs inside the only directory the QLever container
    # mounts, without copying them.
    for source, name in ((dataset, INPUT_NAMES[0]), (provenance, INPUT_NAMES[1])):
        (index_dir / name).unlink(missing_ok=True)
        os.link(source, index_dir / name)

    console.print("Extracting the URI associations of the update queries...")
    start = time.perf_counter()
    write_associations(provenance, index_dir / INPUT_NAMES[2])
    association_s = round(time.perf_counter() - start, 2)
    console.print(f"  Association time: {association_s}s")
    with (index_dir / ASSOCIATION_RECORD).open("w", encoding="utf-8") as file:
        json.dump({"qlever_association_s": association_s}, file)


def index_ready(name: str, index_dir: Path, metadata_file: Path) -> bool:
    if not any(index_dir.glob(f"{name}.index.*")) or not metadata_file.is_file():
        return False
    with metadata_file.open(encoding="utf-8") as file:
        return json.load(file)["store_bytes"] > 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("corpus", choices=corpora.CORPUS_NAMES)
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Extract the associations and build the index again",
    )
    args = parser.parse_args()

    corpus = corpora.get(args.corpus)
    index_dir = corpus.dir / "qlever-data"
    metadata_file = corpora.DATA_DIR / f"qlever_ingestion_time_{corpus.name}.json"
    container = f"qlever-{corpus.name}"
    console.print(f"=== QLever setup ({corpus.name}, port {corpus.port}) ===")

    ready = index_dir.is_dir() and index_ready(corpus.name, index_dir, metadata_file)
    if ready and not args.rebuild:
        console.print(f"Reusing existing QLever index in {index_dir}")
    else:
        if args.rebuild and index_dir.exists():
            shutil.rmtree(index_dir)
        index_dir.mkdir(parents=True, exist_ok=True)
        association_record = index_dir / ASSOCIATION_RECORD
        if association_record.is_file():
            console.print(f"Reusing the URI associations in {index_dir}")
        else:
            extract_associations(corpus, index_dir)
        with association_record.open(encoding="utf-8") as file:
            association_s = json.load(file)["qlever_association_s"]

        console.print("Building the QLever index...")
        start = time.perf_counter()
        build_index(corpus.name, index_dir, container)
        index_s = round(time.perf_counter() - start, 2)
        console.print(f"  Index time: {index_s}s")

        metadata = {
            "qlever_association_s": association_s,
            "qlever_index_s": index_s,
            "store_bytes": store_bytes(index_dir),
            "qlever_cli_version": version("qlever"),
            "qlever_image": QLEVER_IMAGE,
            "qlever_image_id": docker_image_id(QLEVER_IMAGE),
            "stxxl_memory": STXXL_MEMORY,
            "memory_for_queries": MEMORY_FOR_QUERIES,
            "cache_max_size": CACHE_MAX_SIZE,
        }
        with metadata_file.open("w", encoding="utf-8") as file:
            json.dump(metadata, file, indent=2)

    start_server(corpus, index_dir, container)
    console.print(f"\nQLever is running at {corpus.endpoint()}")
    console.print(f"To stop: docker stop {container}")
    console.print(f"To rebuild: {sys.argv[0]} {corpus.name} --rebuild")
    console.print(
        "Next: uv run --group benchmark python benchmark/bear/verify_results.py "
        f"--corpus {corpus.name}"
    )


if __name__ == "__main__":
    main()
