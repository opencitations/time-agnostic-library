# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import logging
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import cast

from pyoxigraph import Literal, NamedNode, Quad, RdfFormat, parse, serialize
from sparqlite import SPARQLClient

from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.qlever import generate_qlever_index
from time_agnostic_library.support import generate_config_file

logger = logging.getLogger(__name__)


def _iter_qlever_input(
    dataset_files: list[Path], provenance_files: list[Path]
) -> Iterator[Quad]:
    predicate = NamedNode(ProvEntity.iri_has_update_query)
    graph = NamedNode("urn:tal:qlever:prov/")
    for path in provenance_files:
        for quad in parse(path=path, format=RdfFormat.N_QUADS):
            yield quad
            if quad.predicate == predicate:
                associations = generate_qlever_index(
                    [
                        (
                            cast("NamedNode", quad.subject).value,
                            cast("Literal", quad.object).value,
                        )
                    ]
                )
                for s, p, o in associations:
                    yield Quad(
                        NamedNode(str(s)), NamedNode(str(p)), Literal(str(o)), graph
                    )
    for path in dataset_files:
        yield from parse(path=path, format=RdfFormat.N_QUADS)


def setup_qlever(
    dataset_files: list[Path],
    provenance_files: list[Path],
    output_dir: Path,
    port: int = 7000,
) -> Path:
    qlever = shutil.which("qlever")
    if qlever is None:
        msg = 'Install QLever support with uv add "time-agnostic-library[qlever]"'
        raise FileNotFoundError(msg)
    for path in [*dataset_files, *provenance_files]:
        if not path.is_file():
            raise FileNotFoundError(path)
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True)
    logger.info("Preparing dataset, provenance and snapshot URI associations")
    serialize(
        _iter_qlever_input(dataset_files, provenance_files),
        output=output_dir / "tal.nq",
        format=RdfFormat.N_QUADS,
    )
    logger.info("Building QLever index in %s", output_dir)
    subprocess.run(  # noqa: S603
        [
            qlever,
            "index",
            "--name",
            "tal",
            "--format",
            "nq",
            "--input-files",
            "tal.nq",
            "--cat-input-files",
            "cat tal.nq",
            "--settings-json",
            '{"prefixes-external": []}',
            "--system",
            "docker",
        ],
        cwd=output_dir,
        check=True,
    )
    logger.info("Starting QLever on port %s", port)
    subprocess.run(  # noqa: S603
        [
            qlever,
            "start",
            "--name",
            "tal",
            "--description",
            "TAL dataset",
            "--host-name",
            "localhost",
            "--port",
            str(port),
            "--server-container",
            f"tal-qlever-{port}",
            "--access-token",
            "",
            "--system",
            "docker",
            "--no-warmup",
        ],
        cwd=output_dir,
        check=True,
    )
    endpoint = f"http://localhost:{port}"
    with SPARQLClient(endpoint) as client:
        client.ask("ASK { ?s ?p ?o }")
    config_path = output_dir / "config.json"
    generate_config_file(
        str(config_path),
        dataset_urls=[endpoint],
        provenance_urls=[endpoint],
        qlever_full_text_search=True,
    )
    logger.info("Endpoint: %s; TAL configuration: %s", endpoint, config_path)
    return config_path


def main():  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="Build and serve a QLever index for Time Agnostic Library."
    )
    parser.add_argument(
        "--dataset",
        nargs="+",
        type=Path,
        required=True,
        help="Current dataset files in N-Quads format",
    )
    parser.add_argument(
        "--provenance",
        nargs="+",
        type=Path,
        required=True,
        help="OCDM provenance files in N-Quads format",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="New directory for the index and TAL configuration",
    )
    parser.add_argument("--port", type=int, default=7000)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    setup_qlever(args.dataset, args.provenance, args.output_dir, args.port)


if __name__ == "__main__":  # pragma: no cover
    main()
