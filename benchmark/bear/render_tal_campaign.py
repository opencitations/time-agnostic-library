# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).parent))

DATA_DIR = Path(__file__).parent / "data"
QUERY_TYPES = ("vm", "sd", "cv")
QUERY_LABELS = {"vm": "SV", "sd": "SD", "cv": "CV"}


def _load(path: Path) -> dict:
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def _validate(datasets: dict[str, dict], memory: dict[str, dict]) -> None:
    hardware = None
    revision = None
    store = None
    for corpus_name, timed in datasets.items():
        traced = memory[corpus_name]
        if timed["hardware"] != traced["hardware"]:
            message = f"Time and memory hardware differ for {corpus_name}"
            raise ValueError(message)
        if timed["protocol"]["manifest_hash"] != traced["protocol"]["manifest_hash"]:
            message = f"Time and memory manifests differ for {corpus_name}"
            raise ValueError(message)
        if timed["protocol"]["git_revision"] != traced["protocol"]["git_revision"]:
            message = f"Time and memory revisions differ for {corpus_name}"
            raise ValueError(message)
        if timed["protocol"]["git_dirty"] or traced["protocol"]["git_dirty"]:
            message = f"Campaign results use a dirty checkout for {corpus_name}"
            raise ValueError(message)
        current_hardware = timed["hardware"]
        current_revision = timed["protocol"]["git_revision"]
        current_store = timed["protocol"]["store"]
        if current_store != traced["protocol"]["store"]:
            message = f"Time and memory stores differ for {corpus_name}"
            raise ValueError(message)
        hardware = current_hardware if hardware is None else hardware
        revision = current_revision if revision is None else revision
        store = current_store if store is None else store
        if (
            current_hardware != hardware
            or current_revision != revision
            or current_store != store
        ):
            message = "Campaign corpora use different hardware, revisions, or stores"
            raise ValueError(message)


def _p95(values: list[float]) -> float:
    if len(values) == 1:
        return values[0]
    return statistics.quantiles(values, n=20, method="inclusive")[18]


def _aggregate(entries: list[dict], key: str) -> dict[str, float | int]:
    values = [entry[key] for entry in entries if entry["status"] == "ok"]
    if len(values) != len(entries):
        message = "Campaign contains failed or mismatched cases"
        raise ValueError(message)
    return {
        "count": len(values),
        "mean": statistics.mean(values),
        "median": statistics.median(values),
        "p95": _p95(values),
        "max": max(values),
    }


def _latex_table(datasets: dict[str, dict], memory: dict[str, dict]) -> str:
    names = {
        "bear-a": "BEAR-A",
        "bear-b-daily": "BEAR-B-daily",
        "bear-b-hourly": "BEAR-B-hourly",
    }
    rows = []
    memory_rows = []
    for corpus_name, data in datasets.items():
        for query_type in QUERY_TYPES:
            stats = _aggregate(data["results"][query_type], "median_s")
            rows.append(
                f"{names[corpus_name]} & {QUERY_LABELS[query_type]} & "
                f"{stats['count']:,} & {stats['mean'] * 1000:.1f} & "
                f"{stats['median'] * 1000:.1f} & {stats['p95'] * 1000:.1f} & "
                f"{stats['max'] * 1000:.1f} \\\\"
            )
            memory_stats = _aggregate(
                memory[corpus_name]["results"][query_type],
                "median_memory_bytes",
            )
            memory_rows.append(
                f"{names[corpus_name]} & {QUERY_LABELS[query_type]} & "
                f"{memory_stats['count']:,} & "
                f"{memory_stats['mean'] / 1_000_000:.1f} & "
                f"{memory_stats['median'] / 1_000_000:.1f} & "
                f"{memory_stats['p95'] / 1_000_000:.1f} & "
                f"{memory_stats['max'] / 1_000_000:.1f} \\\\"
            )
    return "\n".join(
        (
            "\\begin{table*}[!htb]",
            "\\centering",
            "\\caption{Time Agnostic Library execution times under the common "
            "BEAR protocol.}",
            "\\label{tab:tal_results}",
            "\\begin{tabular}{llrrrrr}",
            "\\toprule",
            "Dataset & Query & Count & Mean (ms) & Median (ms) & p95 (ms) & "
            "Max (ms) \\\\",
            "\\midrule",
            *rows,
            "\\bottomrule",
            "\\end{tabular}",
            "\\end{table*}",
            "",
            "\\begin{table*}[!htb]",
            "\\centering",
            "\\caption{Time Agnostic Library peak Python heap allocation.}",
            "\\label{tab:memory}",
            "\\begin{tabular}{llrrrrr}",
            "\\toprule",
            "Dataset & Query & Count & Mean (MB) & Median (MB) & p95 (MB) & "
            "Max (MB) \\\\",
            "\\midrule",
            *memory_rows,
            "\\bottomrule",
            "\\end{tabular}",
            "\\end{table*}",
            "",
            "\\begin{figure*}[!htb]",
            "\\centering",
            "\\includegraphics[width=\\textwidth]{figures/Figure3.jpg}",
            "\\caption{SV median execution time by normalized version position.}",
            "\\label{fig:sv_comparison}",
            "\\end{figure*}",
            "",
            "\\begin{figure*}[!htb]",
            "\\centering",
            "\\includegraphics[width=\\textwidth]{figures/Figure4.jpg}",
            "\\caption{SD median execution time by normalized end-version position.}",
            "\\label{fig:sd_comparison}",
            "\\end{figure*}",
            "",
            "\\begin{figure*}[!htb]",
            "\\centering",
            "\\includegraphics[width=0.7\\textwidth]{figures/Figure5.jpg}",
            "\\caption{Distribution of CV median execution times by corpus.}",
            "\\label{fig:cv_comparison}",
            "\\end{figure*}",
        )
    )


def _series(entries: list[dict], coordinate: str) -> tuple[list[float], list[float]]:
    by_position: dict[float, list[float]] = defaultdict(list)
    maximum = max(entry[coordinate] for entry in entries)
    for entry in entries:
        position = entry[coordinate] / maximum if maximum else 0.0
        by_position[position].append(entry["median_s"] * 1000)
    points = sorted(by_position)
    return points, [statistics.median(by_position[point]) for point in points]


def _render_figures(datasets: dict[str, dict], figure_dir: Path) -> None:
    figure_dir.mkdir(parents=True, exist_ok=True)
    labels = {
        "bear-a": "BEAR-A",
        "bear-b-daily": "BEAR-B-daily",
        "bear-b-hourly": "BEAR-B-hourly",
    }
    for query_type, coordinate, filename in (
        ("vm", "version_index", "Figure3.jpg"),
        ("sd", "version_end", "Figure4.jpg"),
    ):
        figure, axis = plt.subplots(figsize=(10, 4.8))
        for corpus_name, data in datasets.items():
            x, y = _series(data["results"][query_type], coordinate)
            axis.plot(x, y, label=labels[corpus_name])
        axis.set_xlabel("Normalized version position")
        axis.set_ylabel("Median time (ms)")
        axis.set_yscale("log")
        axis.legend()
        figure.tight_layout()
        figure.savefig(figure_dir / filename, dpi=300)
        plt.close(figure)
    figure, axis = plt.subplots(figsize=(7, 4.8))
    values = [
        [entry["median_s"] * 1000 for entry in data["results"]["cv"]]
        for data in datasets.values()
    ]
    axis.boxplot(values, tick_labels=[labels[name] for name in datasets])
    axis.set_ylabel("Median time (ms)")
    axis.set_yscale("log")
    figure.tight_layout()
    figure.savefig(figure_dir / "Figure5.jpg", dpi=300)
    plt.close(figure)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-dir", type=Path, required=True)
    args = parser.parse_args()
    datasets = {
        name: _load(DATA_DIR / f"benchmark_results_{name}.json")
        for name in ("bear-a", "bear-b-daily", "bear-b-hourly")
    }
    memory = {
        name: _load(DATA_DIR / f"benchmark_memory_results_{name}.json")
        for name in datasets
    }
    _validate(datasets, memory)
    generated = args.paper_dir / "generated"
    generated.mkdir(parents=True, exist_ok=True)
    (generated / "bear_results.tex").write_text(
        _latex_table(datasets, memory), encoding="utf-8"
    )
    _render_figures(datasets, args.paper_dir / "figures")


if __name__ == "__main__":
    main()
