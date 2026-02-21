from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.patches import Patch
import numpy as np
from rich.console import Console

from analyze_results import (
    DATA_DIR,
    _format_log_axis,
    _save_plot,
    load_measured_ostrich_results,
    load_ostrich_dm_by_version,
    load_ostrich_vm_by_version,
    load_ostrich_vq_median,
    load_results,
    load_tal_dm_by_version,
    load_tal_vm_by_version,
    load_tal_vq_median,
)

console = Console()

# Line styles: system distinguished by color, granularity by dash pattern.
# No markers. Dashes for hourly use long on/off segments to stay readable.
STYLES = {
    "tal_daily":  {"color": "#0072B2", "linestyle": "-",          "label": "TAL daily (89 ver.)"},
    "tal_hourly": {"color": "#0072B2", "linestyle": (0, (8, 4)),  "label": "TAL hourly (1,299 ver.)"},
    "ost_daily":  {"color": "#D55E00", "linestyle": "-",          "label": "OSTRICH daily (89 ver.)"},
    "ost_hourly": {"color": "#D55E00", "linestyle": (0, (8, 4)),  "label": "OSTRICH hourly (1,299 ver.)"},
}


def _load_granularity_data(granularity: str) -> tuple[dict, list[Path]]:
    results_file = DATA_DIR / f"benchmark_results_{granularity}.json"
    ostrich_results_file = DATA_DIR / f"ostrich_benchmark_results_{granularity}.json"
    ostrich_raw_files = [DATA_DIR / f"ostrich_raw_{pt}_{granularity}.txt" for pt in ["p", "po"]]
    data = load_results(results_file)
    load_measured_ostrich_results(ostrich_results_file)
    return data, ostrich_raw_files


def _normalize_keys(data: dict[int, float]) -> tuple[list[float], list[float]]:
    versions = sorted(data.keys())
    max_v = max(versions)
    pct = [v / max_v * 100 for v in versions]
    vals = [data[v] for v in versions]
    return pct, vals


def _plot_line(ax: Axes, pct: list[float], vals: list[float], style_key: str) -> None:
    s = STYLES[style_key]
    ax.plot(pct, vals, color=s["color"], linestyle=s["linestyle"],
            linewidth=1.5, label=s["label"])


def _plot_line_chart(ax: Axes,
                     daily_data: dict[int, float], hourly_data: dict[int, float],
                     daily_ost_data: dict[int, float] | None,
                     hourly_ost_data: dict[int, float] | None) -> None:
    pct_d, vals_d = _normalize_keys(daily_data)
    pct_h, vals_h = _normalize_keys(hourly_data)
    _plot_line(ax, pct_d, vals_d, "tal_daily")
    _plot_line(ax, pct_h, vals_h, "tal_hourly")
    if daily_ost_data:
        pct, vals = _normalize_keys(daily_ost_data)
        _plot_line(ax, pct, vals, "ost_daily")
    if hourly_ost_data:
        pct, vals = _normalize_keys(hourly_ost_data)
        _plot_line(ax, pct, vals, "ost_hourly")


def plot_vm_combined(daily_data: dict, daily_ost: list[Path],
                     hourly_data: dict, hourly_ost: list[Path],
                     plot_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    ost_d = load_ostrich_vm_by_version(daily_ost) if any(f.exists() for f in daily_ost) else None
    ost_h = load_ostrich_vm_by_version(hourly_ost) if any(f.exists() for f in hourly_ost) else None
    _plot_line_chart(ax,
                     load_tal_vm_by_version(daily_data["results"]["vm"]),
                     load_tal_vm_by_version(hourly_data["results"]["vm"]),
                     ost_d, ost_h)
    _format_log_axis(ax)
    ax.set_xlabel("Version (% of total)")
    ax.set_ylabel("Lookup time (ms)")
    ax.set_title("VM: median across all triple patterns")
    ax.legend(fontsize=9, handlelength=3)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    _save_plot(fig, plot_dir, "vm_comparison")


def plot_dm_combined(daily_data: dict, daily_ost: list[Path],
                     hourly_data: dict, hourly_ost: list[Path],
                     plot_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    ost_d = load_ostrich_dm_by_version(daily_ost) if any(f.exists() for f in daily_ost) else None
    ost_h = load_ostrich_dm_by_version(hourly_ost) if any(f.exists() for f in hourly_ost) else None
    _plot_line_chart(ax,
                     load_tal_dm_by_version(daily_data["results"]["dm"]),
                     load_tal_dm_by_version(hourly_data["results"]["dm"]),
                     ost_d, ost_h)
    _format_log_axis(ax)
    ax.set_xlabel("Delta target version (% of total)")
    ax.set_ylabel("Lookup time (ms)")
    ax.set_title("DM: median across all triple patterns from V0")
    ax.legend(fontsize=9, handlelength=3)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    _save_plot(fig, plot_dir, "dm_comparison")


def plot_vq_combined(daily_data: dict, daily_ost: list[Path],
                     hourly_data: dict, hourly_ost: list[Path],
                     plot_dir: Path) -> None:
    fig, ax = plt.subplots(figsize=(7, 5))

    has_ost_d = any(f.exists() for f in daily_ost)
    has_ost_h = any(f.exists() for f in hourly_ost)

    groups = ["TAL"]
    if has_ost_d or has_ost_h:
        groups.append("OSTRICH")
    x = np.arange(len(groups))
    width = 0.35

    daily_vals = [load_tal_vq_median(daily_data["results"]["vq"])]
    hourly_vals = [load_tal_vq_median(hourly_data["results"]["vq"])]
    if has_ost_d:
        daily_vals.append(load_ostrich_vq_median(daily_ost))
    if has_ost_h:
        hourly_vals.append(load_ostrich_vq_median(hourly_ost))

    # Legend uses neutral gray so it does not imply a specific system color.
    # Bars themselves use per-system colors on the x-axis labels.
    bars_d = ax.bar(x - width / 2, daily_vals, width,
                    color=[("#0072B2", "#D55E00")[i] for i in range(len(groups))],
                    edgecolor="black")
    bars_h = ax.bar(x + width / 2, hourly_vals, width,
                    color=[("#0072B2", "#D55E00")[i] for i in range(len(groups))],
                    edgecolor="black", hatch="//")

    legend_handles = [
        Patch(facecolor="white", edgecolor="black", label="Daily (89 ver.)"),
        Patch(facecolor="white", edgecolor="black", hatch="//", label="Hourly (1,299 ver.)"),
    ]
    ax.legend(handles=legend_handles, fontsize=9)

    for bars in [bars_d, bars_h]:
        for bar in bars:
            val = bar.get_height()
            ax.text(bar.get_x() + bar.get_width() / 2, val, f"{val:.2f}",
                    ha="center", va="bottom", fontsize=8)

    ax.set_xticks(x)
    ax.set_xticklabels(groups)
    _format_log_axis(ax)
    ax.set_ylabel("Lookup time (ms)")
    ax.set_title("VQ: median across all triple patterns")
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    _save_plot(fig, plot_dir, "vq_comparison")


def main() -> None:
    plot_dir = DATA_DIR / "analysis" / "combined" / "plots"
    console.rule("[bold]Loading data")

    daily_data, daily_ost = _load_granularity_data("daily")
    hourly_data, hourly_ost = _load_granularity_data("hourly")

    console.rule("[bold]Generating combined plots")
    plot_vm_combined(daily_data, daily_ost, hourly_data, hourly_ost, plot_dir)
    plot_dm_combined(daily_data, daily_ost, hourly_data, hourly_ost, plot_dir)
    plot_vq_combined(daily_data, daily_ost, hourly_data, hourly_ost, plot_dir)
    console.print("[bold green]Done.[/bold green]")


if __name__ == "__main__":
    main()
