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
    load_r43ples_dm_by_version,
    load_r43ples_vm_by_version,
    load_r43ples_vq_median,
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
    "r43_daily":  {"color": "#009E73", "linestyle": "-",          "label": "R43ples daily (89 ver.)"},
    "r43_hourly": {"color": "#009E73", "linestyle": (0, (8, 4)),  "label": "R43ples hourly (1,299 ver.)"},
}


def _load_granularity_data(granularity: str) -> tuple[dict, list[Path], Path]:
    results_file = DATA_DIR / f"benchmark_results_{granularity}.json"
    ostrich_results_file = DATA_DIR / f"ostrich_benchmark_results_{granularity}.json"
    ostrich_raw_files = [DATA_DIR / f"ostrich_raw_{pt}_{granularity}.txt" for pt in ["p", "po"]]
    r43ples_file = DATA_DIR / f"r43ples_benchmark_results_{granularity}.json"
    data = load_results(results_file)
    load_measured_ostrich_results(ostrich_results_file)
    return data, ostrich_raw_files, r43ples_file


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


def _plot_line(ax: Axes, pct: list[float], vals: list[float], style_key: str) -> None:
    s = STYLES[style_key]
    ax.plot(pct, vals, color=s["color"], linestyle=s["linestyle"],
            linewidth=1.5, label=s["label"])


def _annotate_line_range(ax: Axes, pct: list[float], vals: list[float], color: str,
                         x_pos: float | None = None, below: bool = False) -> None:
    min_val = min(vals)
    max_val = max(vals)
    if x_pos is not None:
        closest_idx = min(range(len(pct)), key=lambda i: abs(pct[i] - x_pos))
        y_val = vals[closest_idx]
    else:
        closest_idx = len(vals) // 2
        y_val = vals[closest_idx]
    label = f"{_format_ms_label(min_val)} \u2192 {_format_ms_label(max_val)}"
    offset = (0, -6) if below else (0, 4)
    va = "top" if below else "bottom"
    ax.annotate(label, xy=(pct[closest_idx], y_val), xytext=offset,
                textcoords="offset points", fontsize=8, color=color,
                ha="center", va=va)


def _plot_line_chart(ax: Axes,
                     daily_data: dict[int, float], hourly_data: dict[int, float],
                     daily_ost_data: dict[int, float] | None,
                     hourly_ost_data: dict[int, float] | None,
                     daily_r43_data: dict[int, float] | None = None,
                     hourly_r43_data: dict[int, float] | None = None) -> None:
    pct_d, vals_d = _normalize_keys(daily_data)
    pct_h, vals_h = _normalize_keys(hourly_data)
    _plot_line(ax, pct_d, vals_d, "tal_daily")
    _annotate_line_range(ax, pct_d, vals_d, STYLES["tal_daily"]["color"], x_pos=20, below=True)
    _plot_line(ax, pct_h, vals_h, "tal_hourly")
    _annotate_line_range(ax, pct_h, vals_h, STYLES["tal_hourly"]["color"], x_pos=75)
    if daily_ost_data:
        pct, vals = _normalize_keys(daily_ost_data)
        _plot_line(ax, pct, vals, "ost_daily")
    if hourly_ost_data:
        pct, vals = _normalize_keys(hourly_ost_data)
        _plot_line(ax, pct, vals, "ost_hourly")
    if daily_r43_data:
        pct, vals = _normalize_keys(daily_r43_data)
        _plot_line(ax, pct, vals, "r43_daily")
        _annotate_line_range(ax, pct, vals, STYLES["r43_daily"]["color"])
    if hourly_r43_data:
        pct, vals = _normalize_keys(hourly_r43_data)
        _plot_line(ax, pct, vals, "r43_hourly")
        _annotate_line_range(ax, pct, vals, STYLES["r43_hourly"]["color"])


def plot_vm_combined(daily_data: dict, daily_ost: list[Path],
                     hourly_data: dict, hourly_ost: list[Path],
                     plot_dir: Path,
                     daily_r43: Path | None = None,
                     hourly_r43: Path | None = None) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    ost_d = load_ostrich_vm_by_version(daily_ost) if any(f.exists() for f in daily_ost) else None
    ost_h = load_ostrich_vm_by_version(hourly_ost) if any(f.exists() for f in hourly_ost) else None
    r43_d = load_r43ples_vm_by_version(daily_r43) if daily_r43 else None
    r43_h = load_r43ples_vm_by_version(hourly_r43) if hourly_r43 else None
    _plot_line_chart(ax,
                     load_tal_vm_by_version(daily_data["results"]["vm"]),
                     load_tal_vm_by_version(hourly_data["results"]["vm"]),
                     ost_d, ost_h,
                     r43_d or None, r43_h or None)
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
                     plot_dir: Path,
                     daily_r43: Path | None = None,
                     hourly_r43: Path | None = None) -> None:
    fig, ax = plt.subplots(figsize=(8, 5))
    ost_d = load_ostrich_dm_by_version(daily_ost) if any(f.exists() for f in daily_ost) else None
    ost_h = load_ostrich_dm_by_version(hourly_ost) if any(f.exists() for f in hourly_ost) else None
    r43_d = load_r43ples_dm_by_version(daily_r43) if daily_r43 else None
    r43_h = load_r43ples_dm_by_version(hourly_r43) if hourly_r43 else None
    _plot_line_chart(ax,
                     load_tal_dm_by_version(daily_data["results"]["dm"]),
                     load_tal_dm_by_version(hourly_data["results"]["dm"]),
                     ost_d, ost_h,
                     r43_d or None, r43_h or None)
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
                     plot_dir: Path,
                     daily_r43: Path | None = None,
                     hourly_r43: Path | None = None) -> None:
    fig, ax = plt.subplots(figsize=(7, 5))

    has_ost_d = any(f.exists() for f in daily_ost)
    has_ost_h = any(f.exists() for f in hourly_ost)
    r43_d_val = load_r43ples_vq_median(daily_r43) if daily_r43 else None
    r43_h_val = load_r43ples_vq_median(hourly_r43) if hourly_r43 else None
    has_r43 = r43_d_val is not None or r43_h_val is not None

    groups = ["TAL"]
    colors = ["#0072B2"]
    if has_ost_d or has_ost_h:
        groups.append("OSTRICH")
        colors.append("#D55E00")
    if has_r43:
        groups.append("R43ples")
        colors.append("#009E73")
    x = np.arange(len(groups))
    width = 0.35

    daily_vals = [load_tal_vq_median(daily_data["results"]["vq"])]
    hourly_vals = [load_tal_vq_median(hourly_data["results"]["vq"])]
    if has_ost_d:
        daily_vals.append(load_ostrich_vq_median(daily_ost))
    if has_ost_h:
        hourly_vals.append(load_ostrich_vq_median(hourly_ost))
    if has_r43:
        daily_vals.append(r43_d_val if r43_d_val is not None else 0)
        hourly_vals.append(r43_h_val if r43_h_val is not None else 0)

    bars_d = ax.bar(x - width / 2, daily_vals, width,
                    color=colors, edgecolor="black")
    bars_h = ax.bar(x + width / 2, hourly_vals, width,
                    color=colors, edgecolor="black", hatch="//")

    legend_handles = [
        Patch(facecolor="white", edgecolor="black", label="Daily (89 ver.)"),
        Patch(facecolor="white", edgecolor="black", hatch="//", label="Hourly (1,299 ver.)"),
    ]
    ax.legend(handles=legend_handles, fontsize=9)

    for bars in [bars_d, bars_h]:
        for bar in bars:
            val = bar.get_height()
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, val,
                        _format_ms_label(val),
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

    daily_data, daily_ost, daily_r43 = _load_granularity_data("daily")
    hourly_data, hourly_ost, hourly_r43 = _load_granularity_data("hourly")

    daily_r43_path = daily_r43 if daily_r43.exists() else None
    hourly_r43_path = hourly_r43 if hourly_r43.exists() else None

    console.rule("[bold]Generating combined plots")
    plot_vm_combined(daily_data, daily_ost, hourly_data, hourly_ost, plot_dir,
                     daily_r43_path, hourly_r43_path)
    plot_dm_combined(daily_data, daily_ost, hourly_data, hourly_ost, plot_dir,
                     daily_r43_path, hourly_r43_path)
    plot_vq_combined(daily_data, daily_ost, hourly_data, hourly_ost, plot_dir,
                     daily_r43_path, hourly_r43_path)
    console.print("[bold green]Done.[/bold green]")


if __name__ == "__main__":
    main()
