# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from analyze_results import (
    DATA_DIR,
    _format_log_axis,
    _save_plot,
    load_measured_ostrich_results,
    load_ostrich_dm_by_version,
    load_ostrich_vm_by_version,
    load_ostrich_vq_median,
    load_r43ples_dm_by_version,
    load_r43ples_vm_by_version,
    load_r43ples_vq_median,
    load_results,
    load_tal_dm_by_version,
    load_tal_vm_by_version,
    load_tal_vq_median,
    validate_manifest,
    validate_result_counts,
)
from matplotlib.axes import Axes
from matplotlib.patches import Patch
from rich.console import Console

console = Console()

CORPORA = {
    "bear-a": {
        "label": "BEAR-A (58 ver.)",
        "linestyle": (0, (1, 2)),
        "hatch": "..",
        "query_sets": (
            "s-low",
            "s-high",
            "p-low",
            "p-high",
            "o-low",
            "o-high",
            "sp-low",
            "sp-high",
            "po-low",
            "po-high",
            "so-low",
            "spo",
        ),
    },
    "bear-b-daily": {
        "label": "daily (89 ver.)",
        "linestyle": "-",
        "hatch": "",
        "query_sets": ("p", "po"),
    },
    "bear-b-hourly": {
        "label": "hourly (1,299 ver.)",
        "linestyle": (0, (8, 4)),
        "hatch": "//",
        "query_sets": ("p", "po"),
    },
}
SYSTEMS = {
    "TAL": "#0072B2",
    "OSTRICH": "#D55E00",
    "R43ples": "#009E73",
}
# Where the min/max annotation of each TAL line sits: (x position, below the line).
TAL_ANNOTATIONS = {
    "bear-a": (60, True),
    "bear-b-daily": (20, False),
    "bear-b-hourly": (75, False),
}


class Corpus:
    def __init__(self, name: str) -> None:
        self.name = name
        self.style = CORPORA[name]
        self.tal = load_results(DATA_DIR / f"benchmark_results_{name}.json")
        ostrich_results = DATA_DIR / f"ostrich_benchmark_results_{name}.json"
        self.ostrich_raw = [
            DATA_DIR / f"ostrich_raw_{query_set}_{name}.txt"
            for query_set in self.style["query_sets"]
        ]
        r43ples_results = DATA_DIR / f"r43ples_benchmark_results_{name}.json"
        self.r43ples = r43ples_results if r43ples_results.exists() else None
        # The OSTRICH evaluator never queries the last ingested version, so the
        # delta grid shared by all systems stops before it.
        self.last_version = max(
            entry["version_index"] for entry in self.tal["results"]["vm"]
        )
        self.comparison_files = [ostrich_results, r43ples_results]
        load_measured_ostrich_results(ostrich_results)

    def validate(self, *, strict: bool) -> None:
        validate_manifest(self.tal, self.comparison_files, strict=strict)
        validate_result_counts(self.tal, self.comparison_files, strict=strict)


def _normalize_keys(data: dict[int, float]) -> tuple[list[float], list[float]]:
    versions = sorted(data.keys())
    max_v = max(versions)
    pct = [v / max_v * 100 for v in versions]
    vals = [data[v] for v in versions]
    return pct, vals


def _format_ms_label(ms: float) -> str:
    if ms >= 60_000:
        return f"{ms / 60_000:.1f} min"
    if ms >= 1_000:
        return f"{ms / 1_000:.1f} s"
    return f"{ms:.1f} ms"


def _plot_line(
    ax: Axes, data: dict[int, float], system: str, corpus: Corpus
) -> tuple[list[float], list[float]]:
    pct, vals = _normalize_keys(data)
    ax.plot(
        pct,
        vals,
        color=SYSTEMS[system],
        linestyle=corpus.style["linestyle"],
        linewidth=1.5,
        label=f"{system} {corpus.style['label']}",
    )
    return pct, vals


def _annotate_line_range(
    ax: Axes,
    pct: list[float],
    vals: list[float],
    color: str,
    *,
    x_pos: float | None = None,
    below: bool = False,
) -> None:
    if x_pos is not None:
        closest_idx = min(range(len(pct)), key=lambda i: abs(pct[i] - x_pos))
    else:
        closest_idx = len(vals) // 2
    label = f"{_format_ms_label(min(vals))} → {_format_ms_label(max(vals))}"
    ax.annotate(
        label,
        xy=(pct[closest_idx], vals[closest_idx]),
        xytext=(0, -6) if below else (0, 4),
        textcoords="offset points",
        fontsize=8,
        color=color,
        ha="center",
        va="top" if below else "bottom",
    )


def _plot_lines(ax: Axes, corpora: list[Corpus], query_type: str) -> None:
    for corpus in corpora:
        if query_type == "vm":
            tal = load_tal_vm_by_version(corpus.tal["results"]["vm"])
            ostrich = load_ostrich_vm_by_version(corpus.ostrich_raw)
            r43ples = (
                load_r43ples_vm_by_version(corpus.r43ples) if corpus.r43ples else {}
            )
        else:
            tal = load_tal_dm_by_version(corpus.tal["results"]["sd"])
            ostrich = load_ostrich_dm_by_version(corpus.ostrich_raw)
            r43ples = (
                load_r43ples_dm_by_version(corpus.r43ples) if corpus.r43ples else {}
            )
            tal.pop(corpus.last_version, None)
            r43ples.pop(corpus.last_version, None)
        pct, vals = _plot_line(ax, tal, "TAL", corpus)
        x_pos, below = TAL_ANNOTATIONS[corpus.name]
        _annotate_line_range(ax, pct, vals, SYSTEMS["TAL"], x_pos=x_pos, below=below)
        _plot_line(ax, ostrich, "OSTRICH", corpus)
        if r43ples:
            pct, vals = _plot_line(ax, r43ples, "R43ples", corpus)
            _annotate_line_range(ax, pct, vals, SYSTEMS["R43ples"])


def plot_vm_combined(corpora: list[Corpus], plot_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    _plot_lines(ax, corpora, "vm")
    _format_log_axis(ax)
    ax.set_xlabel("Version (% of total)")
    ax.set_ylabel("Lookup time (ms)")
    ax.set_title("VM: median across all triple patterns")
    ax.legend(
        fontsize=8,
        handlelength=3,
        ncol=2,
        loc="center left",
        bbox_to_anchor=(0.01, 0.38),
    )
    ax.grid(visible=True, alpha=0.3)
    fig.tight_layout()
    _save_plot(fig, plot_dir, "vm_comparison")


def plot_dm_combined(corpora: list[Corpus], plot_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    _plot_lines(ax, corpora, "dm")
    _format_log_axis(ax)
    ax.set_xlabel("Delta target version (% of total)")
    ax.set_ylabel("Lookup time (ms)")
    ax.set_title("DM: median across all triple patterns from V0")
    ax.legend(
        fontsize=8,
        handlelength=3,
        ncol=2,
        loc="center left",
        bbox_to_anchor=(0.01, 0.38),
    )
    ax.grid(visible=True, alpha=0.3)
    fig.tight_layout()
    _save_plot(fig, plot_dir, "dm_comparison")


def plot_vq_combined(corpora: list[Corpus], plot_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(7, 5))
    groups = list(SYSTEMS)
    x = np.arange(len(groups))
    width = 0.8 / len(corpora)
    for index, corpus in enumerate(corpora):
        values = [
            load_tal_vq_median(corpus.tal["results"]["cv"]),
            load_ostrich_vq_median(corpus.ostrich_raw),
            load_r43ples_vq_median(corpus.r43ples) if corpus.r43ples else None,
        ]
        offsets = x + (index - (len(corpora) - 1) / 2) * width
        bars = ax.bar(
            offsets,
            [value or 0 for value in values],
            width,
            color=list(SYSTEMS.values()),
            edgecolor="black",
            hatch=corpus.style["hatch"],
        )
        for bar, value in zip(bars, values, strict=True):
            if value:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    value,
                    _format_ms_label(value),
                    ha="center",
                    va="bottom",
                    fontsize=8,
                )
    ax.legend(
        handles=[
            Patch(
                facecolor="white",
                edgecolor="black",
                hatch=corpus.style["hatch"],
                label=corpus.style["label"],
            )
            for corpus in corpora
        ],
        fontsize=9,
    )
    ax.set_xticks(x)
    ax.set_xticklabels(groups)
    _format_log_axis(ax)
    ax.set_ylabel("Lookup time (ms)")
    ax.set_title("VQ: median across all triple patterns")
    ax.grid(visible=True, alpha=0.3, axis="y")
    fig.tight_layout()
    _save_plot(fig, plot_dir, "vq_comparison")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--lenient",
        action="store_true",
        help="Report revision and result-count differences between the systems "
        "instead of failing",
    )
    args = parser.parse_args()
    plot_dir = DATA_DIR / "analysis" / "combined" / "plots"
    console.rule("[bold]Loading data")
    corpora = [Corpus(name) for name in CORPORA]
    for corpus in corpora:
        corpus.validate(strict=not args.lenient)
    console.rule("[bold]Generating combined plots")
    plot_vm_combined(corpora, plot_dir)
    plot_dm_combined(corpora, plot_dir)
    plot_vq_combined(corpora, plot_dir)
    console.print("[bold green]Done.[/bold green]")


if __name__ == "__main__":
    main()
