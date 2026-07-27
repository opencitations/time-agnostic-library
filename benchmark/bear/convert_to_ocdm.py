# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import json
import re
import time
from pathlib import Path

import corpora
from rich.console import Console

from time_agnostic_library.ocdm_converter import OCDMConverter

console = Console()

DATA_GRAPH = "http://bear-benchmark.org/data/"
AGENT_URI = "http://bear-benchmark.org/converter"
XSD_NS = "http://www.w3.org/2001/XMLSchema#"

_INTEGER_SUFFIX = f"^^<{XSD_NS}integer>"
_INT_SUFFIX = f"^^<{XSD_NS}int>"
_INTEGER_SUFFIX_LEN = len(_INTEGER_SUFFIX)
_STRING_SUFFIX = f"^^<{XSD_NS}string>"


def normalize_object(obj: str) -> str:
    if obj.endswith(_STRING_SUFFIX):
        return obj[: -len(_STRING_SUFFIX)]
    if obj.endswith(_INTEGER_SUFFIX):
        return obj[:-_INTEGER_SUFFIX_LEN] + _INT_SUFFIX
    return obj


def find_ic_files(ic_dir: Path) -> list[Path]:
    files = sorted(ic_dir.rglob("*.nt"))
    version_files = []
    for f in files:
        match = re.search(r"(\d+)", f.stem)
        if match:
            version_files.append((int(match.group(1)), f))
    version_files.sort(key=lambda x: x[0])
    return [f for _, f in version_files]


def find_cb_files(cb_dir: Path) -> list[tuple[Path, Path]]:
    added_files: dict[int, Path] = {}
    deleted_files: dict[int, Path] = {}
    for f in cb_dir.iterdir():
        match = re.match(r"data-(added|deleted)_(\d+)-(\d+)\.nt$", f.name)
        if not match:
            continue
        change_type = match.group(1)
        source_version = int(match.group(2))
        if change_type == "added":
            added_files[source_version] = f
        else:
            deleted_files[source_version] = f
    versions = sorted(set(added_files.keys()) & set(deleted_files.keys()))
    return [(added_files[v], deleted_files[v]) for v in versions]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--corpus", choices=corpora.CORPUS_NAMES, default="bear-b-daily"
    )
    parser.add_argument("--strategy", choices=["ic", "cb"], default="ic")
    parser.add_argument(
        "--versions",
        type=int,
        default=None,
        help=(
            "Convert only the first N versions, for sizing a corpus before "
            "committing to it. The outputs carry the count in their name, so a "
            "partial conversion cannot be mistaken for a complete one."
        ),
    )
    args = parser.parse_args()

    corpus = corpora.get(args.corpus)
    data_dir = corpus.dir

    converter = OCDMConverter(
        data_graph_uri=DATA_GRAPH,
        agent_uri=AGENT_URI,
        object_normalizer=normalize_object,
    )

    infix = f".{args.versions}v" if args.versions else ""
    # Compressed: the provenance of a large corpus is hundreds of gigabytes of
    # repetitive N-Quads, and both bulk loaders read gzip directly.
    dataset_output = data_dir / f"dataset{infix}.nq.gz"
    provenance_output = data_dir / f"provenance{infix}.nq.gz"

    start = time.perf_counter()

    if args.strategy == "ic":
        ic_dir = data_dir / "IC"
        if not ic_dir.exists():
            msg = f"IC directory not found: {ic_dir}. Run download.py first."
            raise FileNotFoundError(msg)
        ic_files = find_ic_files(ic_dir)[: args.versions]
        num_versions = len(ic_files)
        console.print(f"Found {num_versions} IC versions")
        converter.convert_from_ic(
            ic_files=ic_files,
            timestamps=corpus.datetimes(num_versions),
            dataset_output=dataset_output,
            provenance_output=provenance_output,
        )
    else:
        ic_dir = data_dir / "IC"
        cb_dir = data_dir / "CB"
        if not cb_dir.exists():
            msg = f"CB directory not found: {cb_dir}. Run download.py first."
            raise FileNotFoundError(msg)
        ic_files = find_ic_files(ic_dir)
        if not ic_files:
            msg = f"No IC files found in {ic_dir}. Need initial snapshot."
            raise FileNotFoundError(msg)
        initial_snapshot = ic_files[0]
        changesets = find_cb_files(cb_dir)
        num_versions = len(changesets) + 1
        console.print(
            f"Found initial snapshot + {len(changesets)} CB changesets "
            f"({num_versions} versions)"
        )
        converter.convert_from_cb(
            initial_snapshot=initial_snapshot,
            changesets=changesets,
            timestamps=corpus.datetimes(num_versions),
            dataset_output=dataset_output,
            provenance_output=provenance_output,
        )

    elapsed_s = time.perf_counter() - start

    console.print(f"\nConversion complete ({args.strategy.upper()} strategy):")
    console.print(
        f"  Dataset: {dataset_output} ({dataset_output.stat().st_size / 1024:.1f} KB)"
    )
    console.print(
        f"  Provenance: {provenance_output} "
        f"({provenance_output.stat().st_size / 1024:.1f} KB)"
    )

    timing_file = corpora.DATA_DIR / f"ocdm_conversion_time_{corpus.name}{infix}.json"
    timing_file.parent.mkdir(parents=True, exist_ok=True)
    timing_data = {
        "ocdm_conversion_s": round(elapsed_s, 2),
        "strategy": args.strategy,
        "versions": num_versions,
        "dataset_bytes": dataset_output.stat().st_size,
        "provenance_bytes": provenance_output.stat().st_size,
    }
    with timing_file.open("w", encoding="utf-8") as f:
        json.dump(timing_data, f, indent=2)
    console.print(f"Conversion time: {elapsed_s:.2f}s (saved to {timing_file})")


if __name__ == "__main__":
    main()
