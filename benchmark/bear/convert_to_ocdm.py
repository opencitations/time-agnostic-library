import argparse
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from rich.console import Console

from time_agnostic_library.ocdm_converter import OCDMConverter

console = Console()

DATA_GRAPH = "http://bear-benchmark.org/data/"
AGENT_URI = "http://bear-benchmark.org/converter"
XSD_NS = "http://www.w3.org/2001/XMLSchema#"

_INTEGER_SUFFIX = f"^^<{XSD_NS}integer>"
_INT_SUFFIX = f"^^<{XSD_NS}int>"
_INTEGER_SUFFIX_LEN = len(_INTEGER_SUFFIX)

BASE_TIMESTAMP = datetime(2015, 8, 1, 0, 0, 0, tzinfo=timezone.utc)

GRANULARITY_INTERVALS = {
    "daily": timedelta(days=1),
    "hourly": timedelta(hours=1),
    "instant": timedelta(minutes=1),
}

SCRIPT_DIR = Path(__file__).parent


def normalize_object(obj: str) -> str:
    if obj.endswith(_INTEGER_SUFFIX):
        return obj[:-_INTEGER_SUFFIX_LEN] + _INT_SUFFIX
    return obj


def find_ic_files(ic_dir: Path) -> list[Path]:
    files = sorted(ic_dir.rglob(pattern="*.nt")) + sorted(ic_dir.rglob("*.nt.gz"))
    if not files:
        files = sorted(ic_dir.rglob("*.ntriples"))
    version_files = []
    for f in files:
        stem: str = f.stem.replace(".nt", "") if f.suffix == ".gz" else f.stem
        match = re.search(r'(\d+)', stem)
        if match:
            version_files.append((int(match.group(1)), f))
    version_files.sort(key=lambda x: x[0])
    return [f for _, f in version_files]


def find_cb_files(cb_dir: Path) -> list[tuple[Path, Path]]:
    added_files: dict[int, Path] = {}
    deleted_files: dict[int, Path] = {}
    for f in cb_dir.iterdir():
        match = re.match(r'data-(added|deleted)_(\d+)-(\d+)\.nt(?:\.gz)?$', f.name)
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
    parser.add_argument("--granularity", choices=["daily", "hourly", "instant"], default="daily")
    parser.add_argument("--strategy", choices=["ic", "cb"], default="ic")
    args = parser.parse_args()

    interval = GRANULARITY_INTERVALS[args.granularity]
    data_dir = SCRIPT_DIR / "data" / args.granularity

    converter = OCDMConverter(
        data_graph_uri=DATA_GRAPH,
        agent_uri=AGENT_URI,
        object_normalizer=normalize_object,
    )

    dataset_output = data_dir / "dataset.nq"
    provenance_output = data_dir / "provenance.nq"

    start = time.perf_counter()

    if args.strategy == "ic":
        ic_dir = data_dir / "IC"
        if not ic_dir.exists():
            raise FileNotFoundError(f"IC directory not found: {ic_dir}. Run download.py first.")
        ic_files = find_ic_files(ic_dir)
        num_versions = len(ic_files)
        console.print(f"Found {num_versions} IC versions")
        timestamps = [BASE_TIMESTAMP + interval * i for i in range(num_versions)]
        converter.convert_from_ic(
            ic_files=ic_files,
            timestamps=timestamps,
            dataset_output=dataset_output,
            provenance_output=provenance_output,
        )
    else:
        ic_dir = data_dir / "IC"
        cb_dir = data_dir / "CB"
        if not cb_dir.exists():
            raise FileNotFoundError(f"CB directory not found: {cb_dir}. Run download.py first.")
        ic_files = find_ic_files(ic_dir)
        if not ic_files:
            raise FileNotFoundError(f"No IC files found in {ic_dir}. Need initial snapshot.")
        initial_snapshot = ic_files[0]
        changesets = find_cb_files(cb_dir)
        num_versions = len(changesets) + 1
        console.print(f"Found initial snapshot + {len(changesets)} CB changesets ({num_versions} versions)")
        timestamps = [BASE_TIMESTAMP + interval * i for i in range(num_versions)]
        converter.convert_from_cb(
            initial_snapshot=initial_snapshot,
            changesets=changesets,
            timestamps=timestamps,
            dataset_output=dataset_output,
            provenance_output=provenance_output,
        )

    elapsed_s = time.perf_counter() - start

    console.print(f"\nConversion complete ({args.strategy.upper()} strategy):")
    console.print(f"  Dataset: {dataset_output} ({dataset_output.stat().st_size / 1024:.1f} KB)")
    console.print(f"  Provenance: {provenance_output} ({provenance_output.stat().st_size / 1024:.1f} KB)")

    timing_file = SCRIPT_DIR / "data" / f"ocdm_conversion_time_{args.granularity}.json"
    timing_file.parent.mkdir(parents=True, exist_ok=True)
    with open(timing_file, "w", encoding="utf-8") as f:
        json.dump({"ocdm_conversion_s": round(elapsed_s, 2), "strategy": args.strategy}, f, indent=2)
    console.print(f"Conversion time: {elapsed_s:.2f}s (saved to {timing_file})")


if __name__ == "__main__":
    main()
