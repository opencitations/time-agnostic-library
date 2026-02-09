import argparse
import csv
import json
import re
import statistics
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, NullFormatter
from rich.console import Console
from rich.table import Table

console = Console()

DATA_DIR = Path(__file__).parent / "data"

PUBLISHED_RESULTS = {
    "Jena-IC": {
        "source": "Fernandez et al., SWJ 2019",
        "vm_ms": {"p": 136, "po": 53},
        "dm_ms": None,
        "vq_ms": {"p": 83, "po": 19},
        "ingestion_s": None,
        "notes": "Index build required",
    },
    "Jena-CB": {
        "source": "Fernandez et al., SWJ 2019",
        "vm_ms": None,
        "dm_ms": None,
        "vq_ms": {"p": 19, "po": 4},
        "ingestion_s": None,
        "notes": "Index build required",
    },
    "HDT-IC": {
        "source": "Fernandez et al., SWJ 2019",
        "vm_ms": {"p": 11.58, "po": 0.83},
        "dm_ms": None,
        "vq_ms": {"p": 6.57, "po": 2.34},
        "ingestion_s": None,
        "notes": "HDT compression required",
    },
    "HDT-CB": {
        "source": "Fernandez et al., SWJ 2019",
        "vm_ms": None,
        "dm_ms": None,
        "vq_ms": {"p": 0.43, "po": 0.08},
        "ingestion_s": None,
        "notes": "HDT compression required",
    },
    "OSTRICH": {
        "source": "Taelman et al., JWS 2018",
        "vm_ms": {"avg": 0.71},
        "dm_ms": {"avg": 0.38},
        "vq_ms": {"avg": 0.90},
        "ingestion_s": 742,
        "notes": "C++ native storage engine",
    },
    "v-RDFCSA": {
        "source": "Cerdeira-Pena et al., KAIS 2024",
        "vm_ms": {"avg": 1.0},
        "dm_ms": {"avg": 1.0},
        "vq_ms": {"avg": 1.0},
        "ingestion_s": None,
        "notes": "Compression time required",
    },
    "TrieDF": {
        "source": "Cerdeira-Pena et al., CEUR 2022",
        "vm_ms": {"avg": 0.1},
        "dm_ms": {"avg": 0.1},
        "vq_ms": {"avg": 0.01},
        "ingestion_s": 16074,
        "notes": "16074s ingestion for BEAR-B",
    },
}


def load_results(filepath: Path) -> dict:
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def compute_aggregates(results: List[dict]) -> dict:
    valid_means = [r["mean_s"] * 1000 for r in results if r["mean_s"] is not None]
    if not valid_means:
        return {"count": 0}
    return {
        "count": len(results),
        "mean_ms": statistics.mean(valid_means),
        "median_ms": statistics.median(valid_means),
        "std_ms": statistics.stdev(valid_means) if len(valid_means) > 1 else 0.0,
        "min_ms": min(valid_means),
        "max_ms": max(valid_means),
    }


def compute_aggregates_by_pattern(results: List[dict]) -> Dict[str, dict]:
    by_pattern = {}
    for r in results:
        pt = r.get("pattern_type", "unknown")
        by_pattern.setdefault(pt, []).append(r)
    return {pt: compute_aggregates(rs) for pt, rs in by_pattern.items()}


def compute_break_even(
    tal_mean_ms: float,
    other_mean_ms: float,
    ingestion_s: float,
) -> Optional[float]:
    diff_ms = tal_mean_ms - other_mean_ms
    if diff_ms <= 0:
        return None
    ingestion_ms = ingestion_s * 1000
    return ingestion_ms / diff_ms


def _median_ms(entries: List[dict], key: str = "median_s", scale: float = 1000) -> float:
    return statistics.median(r[key] * scale for r in entries if r[key] is not None)


def _group_by(entries: List[dict], field: str) -> Dict:
    groups: Dict = {}
    for r in entries:
        groups.setdefault(r[field], []).append(r)
    return groups


def load_tal_vm_by_version(vm_results: List[dict], pattern_filter: str | None = None) -> Dict[int, float]:
    filtered = [r for r in vm_results if r["median_s"] is not None]
    if pattern_filter:
        filtered = [r for r in filtered if r["pattern_type"] == pattern_filter]
    by_version = _group_by(filtered, "version_index")
    return {v: _median_ms(entries) for v, entries in sorted(by_version.items())}


def load_tal_dm_by_version(dm_results: List[dict], pattern_filter: str | None = None) -> Dict[int, float]:
    filtered = [r for r in dm_results if r["median_s"] is not None]
    if pattern_filter:
        filtered = [r for r in filtered if r["pattern_type"] == pattern_filter]
    by_end = _group_by(filtered, "version_end")
    return {v: _median_ms(entries) for v, entries in sorted(by_end.items())}


def load_tal_vq_median(vq_results: List[dict], pattern_filter: str | None = None) -> float:
    filtered = [r for r in vq_results if r["median_s"] is not None]
    if pattern_filter:
        filtered = [r for r in filtered if r["pattern_type"] == pattern_filter]
    return _median_ms(filtered)


# --- OSTRICH raw file parsing ---

def _parse_ostrich_raw_files(raw_files: List[Path]) -> List[dict]:
    patterns = []
    for raw_file in raw_files:
        if not raw_file.exists():
            continue
        pattern_type = raw_file.stem.replace("ostrich_raw_", "")
        current_pattern = None
        current_section = None
        for line in raw_file.read_text().splitlines():
            line = line.strip()
            match = re.match(r"---PATTERN START:\s*(.+)", line)
            if match:
                current_pattern = {"pattern_type": pattern_type, "vm": [], "dm": [], "vq": []}
                patterns.append(current_pattern)
                current_section = None
                continue
            if "---VERSION MATERIALIZED" in line:
                current_section = "vm"
                continue
            if "---DELTA MATERIALIZED" in line:
                current_section = "dm"
                continue
            if line.startswith("--- ---VERSION"):
                current_section = "vq"
                continue
            if line.startswith("---") or line.startswith("patch,") or line.startswith("patch_start,") or line.startswith("offset,"):
                continue
            if current_pattern is None or current_section is None:
                continue
            parts = line.split(",")
            if current_section == "vm" and len(parts) >= 7:
                current_pattern["vm"].append({"patch": int(parts[0]), "median_us": float(parts[4])})
            elif current_section == "dm" and len(parts) >= 8:
                current_pattern["dm"].append({
                    "patch_start": int(parts[0]), "patch_end": int(parts[1]), "median_us": float(parts[5]),
                })
            elif current_section == "vq" and len(parts) >= 5:
                current_pattern["vq"].append({"median_us": float(parts[2])})
    return patterns


def _ostrich_median_ms_by_version(patterns: List[dict], query_type: str, version_field: str,
                                  pattern_filter: str | None = None, start_filter: int | None = None) -> Dict[int, float]:
    by_version: Dict[int, list] = {}
    for pat in patterns:
        if pattern_filter and pat["pattern_type"] != pattern_filter:
            continue
        for entry in pat[query_type]:
            if start_filter is not None and entry.get("patch_start") != start_filter:
                continue
            v = entry[version_field]
            by_version.setdefault(v, []).append(entry["median_us"] / 1000)
    return {v: statistics.median(vals) for v, vals in sorted(by_version.items())}


def load_ostrich_vm_by_version(raw_files: List[Path], pattern_filter: str | None = None) -> Dict[int, float]:
    patterns = _parse_ostrich_raw_files(raw_files)
    return _ostrich_median_ms_by_version(patterns, "vm", "patch", pattern_filter)


def load_ostrich_dm_by_version(raw_files: List[Path], pattern_filter: str | None = None) -> Dict[int, float]:
    patterns = _parse_ostrich_raw_files(raw_files)
    return _ostrich_median_ms_by_version(patterns, "dm", "patch_end", pattern_filter, start_filter=0)


def load_ostrich_vq_median(raw_files: List[Path], pattern_filter: str | None = None) -> float:
    patterns = _parse_ostrich_raw_files(raw_files)
    medians = []
    for pat in patterns:
        if pattern_filter and pat["pattern_type"] != pattern_filter:
            continue
        for entry in pat["vq"]:
            medians.append(entry["median_us"] / 1000)
    return statistics.median(medians)


# --- Plotting ---

def _ms_formatter(val: float, _pos: int) -> str:
    if val >= 1:
        return f"{val:g}"
    return f"{val:.2g}"


def _format_log_axis(ax: plt.Axes) -> None:
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(_ms_formatter))
    ax.yaxis.set_minor_formatter(NullFormatter())


def _save_plot(fig: plt.Figure, plot_dir: Path, name: str) -> None:
    plot_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(plot_dir / f"{name}.pdf", bbox_inches="tight")
    plt.close(fig)
    console.print(f"  Saved: {plot_dir / name}.pdf")


def plot_vm_comparison(tal_vm: List[dict], ostrich_raw_files: List[Path], plot_dir: Path) -> None:
    tal_data = load_tal_vm_by_version(tal_vm)
    fig, ax = plt.subplots(figsize=(8, 5))
    versions = sorted(tal_data.keys())
    ax.plot(versions, [tal_data[v] for v in versions], label="TAL", marker="", linewidth=1.5)
    if any(f.exists() for f in ostrich_raw_files):
        ost_data = load_ostrich_vm_by_version(ostrich_raw_files)
        ost_versions = sorted(ost_data.keys())
        ax.plot(ost_versions, [ost_data[v] for v in ost_versions], label="OSTRICH", marker="", linewidth=1.5)
    _format_log_axis(ax)
    ax.set_xlabel("Version")
    ax.set_ylabel("Lookup time (ms)")
    ax.set_title("VM: median across all triple patterns")
    ax.legend()
    ax.grid(True, alpha=0.3)
    _save_plot(fig, plot_dir, "vm_comparison")


def plot_dm_comparison(tal_dm: List[dict], ostrich_raw_files: List[Path], plot_dir: Path) -> None:
    tal_data = load_tal_dm_by_version(tal_dm)
    fig, ax = plt.subplots(figsize=(8, 5))
    versions = sorted(tal_data.keys())
    ax.plot(versions, [tal_data[v] for v in versions], label="TAL", marker="o", linewidth=1.5, markersize=4)
    if any(f.exists() for f in ostrich_raw_files):
        ost_data = load_ostrich_dm_by_version(ostrich_raw_files)
        ost_versions = sorted(ost_data.keys())
        ax.plot(ost_versions, [ost_data[v] for v in ost_versions], label="OSTRICH", marker="", linewidth=1.5)
    _format_log_axis(ax)
    ax.set_xlabel("Version (delta from V0)")
    ax.set_ylabel("Lookup time (ms)")
    ax.set_title("DM: median across all triple patterns from V0")
    ax.legend()
    ax.grid(True, alpha=0.3)
    _save_plot(fig, plot_dir, "dm_comparison")


def plot_vq_comparison(tal_vq: List[dict], ostrich_raw_files: List[Path], plot_dir: Path) -> None:
    systems = ["TAL"]
    values = [load_tal_vq_median(tal_vq)]
    if any(f.exists() for f in ostrich_raw_files):
        systems.append("OSTRICH")
        values.append(load_ostrich_vq_median(ostrich_raw_files))
    fig, ax = plt.subplots(figsize=(6, 5))
    bars = ax.bar(systems, values)
    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"{val:.2f}",
                ha="center", va="bottom", fontsize=9)
    _format_log_axis(ax)
    ax.set_ylabel("Lookup time (ms)")
    ax.set_title("VQ: median across all triple patterns")
    ax.grid(True, alpha=0.3, axis="y")
    _save_plot(fig, plot_dir, "vq_comparison")


def plot_by_pattern(tal_results: List[dict], ostrich_raw_files: List[Path],
                    query_type: str, load_tal_fn, load_ost_fn, plot_dir: Path,
                    x_label: str, version_key: str) -> None:
    has_ostrich = any(f.exists() for f in ostrich_raw_files)
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), sharey=True)
    for i, pt in enumerate(["p", "po"]):
        ax = axes[i]
        tal_data = load_tal_fn(tal_results, pattern_filter=pt)
        versions = sorted(tal_data.keys())
        ax.plot(versions, [tal_data[v] for v in versions], label="TAL", linewidth=1.5)
        if has_ostrich:
            ost_data = load_ost_fn(ostrich_raw_files, pattern_filter=pt)
            ost_versions = sorted(ost_data.keys())
            ax.plot(ost_versions, [ost_data[v] for v in ost_versions], label="OSTRICH", linewidth=1.5)
        _format_log_axis(ax)
        ax.set_xlabel(x_label)
        ax.set_title(f"?{'P?' if pt == 'p' else 'PO'} patterns")
        ax.legend()
        ax.grid(True, alpha=0.3)
    axes[0].set_ylabel("Lookup time (ms)")
    fig.suptitle(f"{query_type.upper()}: median by pattern type", fontsize=13)
    fig.tight_layout()
    _save_plot(fig, plot_dir, f"{query_type}_by_pattern")


def generate_plots(data: dict, ostrich_raw_files: List[Path], plot_dir: Path) -> None:
    results = data.get("results", {})
    vm_results = results.get("vm", [])
    dm_results = results.get("dm", [])
    vq_results = results.get("vq", [])

    if vm_results:
        plot_vm_comparison(vm_results, ostrich_raw_files, plot_dir)
        plot_by_pattern(vm_results, ostrich_raw_files, "vm",
                        load_tal_vm_by_version, load_ostrich_vm_by_version,
                        plot_dir, "Version", "version_index")
    if dm_results:
        plot_dm_comparison(dm_results, ostrich_raw_files, plot_dir)
        plot_by_pattern(dm_results, ostrich_raw_files, "dm",
                        load_tal_dm_by_version, load_ostrich_dm_by_version,
                        plot_dir, "Version (delta from V0)", "version_end")
    if vq_results:
        plot_vq_comparison(vq_results, ostrich_raw_files, plot_dir)


def generate_comparison_table(tal_results: dict, ocdm_timing_file: Path, qlever_timing_file: Path) -> List[dict]:
    rows = []
    for system_name, published in PUBLISHED_RESULTS.items():
        row = {
            "system": system_name,
            "source": published["source"],
            "vm_ms": None,
            "dm_ms": None,
            "vq_ms": None,
            "ingestion_s": published["ingestion_s"],
            "break_even_vm": None,
            "break_even_vq": None,
            "notes": published["notes"],
        }
        if published["vm_ms"]:
            if "avg" in published["vm_ms"]:
                row["vm_ms"] = published["vm_ms"]["avg"]
            else:
                row["vm_ms"] = statistics.mean(published["vm_ms"].values())
        if published["dm_ms"]:
            if "avg" in published["dm_ms"]:
                row["dm_ms"] = published["dm_ms"]["avg"]
            else:
                row["dm_ms"] = statistics.mean(published["dm_ms"].values())
        if published["vq_ms"]:
            if "avg" in published["vq_ms"]:
                row["vq_ms"] = published["vq_ms"]["avg"]
            else:
                row["vq_ms"] = statistics.mean(published["vq_ms"].values())

        if published["ingestion_s"]:
            tal_vm = tal_results.get("vm", {}).get("mean_ms")
            if tal_vm and row["vm_ms"]:
                row["break_even_vm"] = compute_break_even(tal_vm, row["vm_ms"], published["ingestion_s"])
            tal_vq = tal_results.get("vq", {}).get("mean_ms")
            if tal_vq and row["vq_ms"]:
                row["break_even_vq"] = compute_break_even(tal_vq, row["vq_ms"], published["ingestion_s"])

        rows.append(row)

    tal_ingestion = load_tal_ingestion_time(ocdm_timing_file, qlever_timing_file)
    tal_row = {
        "system": "TAL (ours)",
        "source": "this work",
        "vm_ms": tal_results.get("vm", {}).get("mean_ms"),
        "dm_ms": tal_results.get("dm", {}).get("mean_ms"),
        "vq_ms": tal_results.get("vq", {}).get("mean_ms"),
        "ingestion_s": tal_ingestion or 0,
        "break_even_vm": None,
        "break_even_vq": None,
        "notes": "OCDM conversion + QLever indexing",
    }
    rows.append(tal_row)
    return rows


def generate_capabilities_table() -> List[dict]:
    return [
        {"system": "Jena-IC", "VM": "Y", "SV": "N", "CV": "N", "DM": "N", "SD": "N", "CD": "N"},
        {"system": "Jena-CB", "VM": "N", "SV": "N", "CV": "N", "DM": "Y", "SD": "N", "CD": "N"},
        {"system": "HDT-IC", "VM": "Y", "SV": "N", "CV": "N", "DM": "N", "SD": "N", "CD": "N"},
        {"system": "HDT-CB", "VM": "N", "SV": "N", "CV": "N", "DM": "Y", "SD": "N", "CD": "N"},
        {"system": "OSTRICH", "VM": "Y", "SV": "N", "CV": "N", "DM": "Y", "SD": "N", "CD": "N"},
        {"system": "v-RDFCSA", "VM": "Y", "SV": "N", "CV": "N", "DM": "Y", "SD": "N", "CD": "N"},
        {"system": "TrieDF", "VM": "Y", "SV": "N", "CV": "N", "DM": "Y", "SD": "N", "CD": "N"},
        {"system": "TAL (ours)", "VM": "Y", "SV": "Y", "CV": "Y", "DM": "Y", "SD": "Y", "CD": "Y"},
    ]


def write_csv(rows: List[dict], filepath: Path) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    console.print(f"  Saved: {filepath}")


def format_val(val, fmt=".2f") -> str:
    if val is None:
        return "---"
    return f"{val:{fmt}}"


def generate_latex_comparison(rows: List[dict], filepath: Path) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        r"\begin{table}[!htb]",
        r"\centering",
        r"\caption{Performance comparison on BEAR-B-daily}",
        r"\label{tab:bear-comparison}",
        r"\begin{tabular}{lrrrrl}",
        r"\toprule",
        r"System & VM (ms) & DM (ms) & VQ (ms) & Ingestion (s) & Break-even (VM) \\",
        r"\midrule",
    ]
    for row in rows:
        system = row["system"]
        vm = format_val(row["vm_ms"])
        dm = format_val(row["dm_ms"])
        vq = format_val(row["vq_ms"])
        ing = format_val(row["ingestion_s"], ".0f")
        be = format_val(row["break_even_vm"], ".0f") if row["break_even_vm"] else "---"
        lines.append(f"{system} & {vm} & {dm} & {vq} & {ing} & {be} \\\\")
    lines.extend([
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ])
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    console.print(f"  Saved: {filepath}")


def generate_latex_capabilities(filepath: Path) -> None:
    rows = generate_capabilities_table()
    filepath.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        r"\begin{table}[!htb]",
        r"\centering",
        r"\caption{Query type support across systems}",
        r"\label{tab:capabilities}",
        r"\begin{tabular}{lcccccc}",
        r"\toprule",
        r"System & VM & SV & CV & DM & SD & CD \\",
        r"\midrule",
    ]
    for row in rows:
        vals = " & ".join(row[k] for k in ["VM", "SV", "CV", "DM", "SD", "CD"])
        lines.append(f"{row['system']} & {vals} \\\\")
    lines.extend([
        r"\bottomrule",
        r"\end{tabular}",
        r"\end{table}",
    ])
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    console.print(f"  Saved: {filepath}")


def print_results_table(tal_aggregates: dict) -> None:
    table = Table(title="TAL benchmark results")
    table.add_column("Query type", style="bold")
    table.add_column("Count", justify="right")
    table.add_column("Mean (ms)", justify="right")
    table.add_column("Median (ms)", justify="right")
    table.add_column("Std (ms)", justify="right")

    for query_type in ["vm", "dm", "vq"]:
        agg = tal_aggregates.get(query_type)
        if agg and agg.get("count", 0) > 0:
            table.add_row(
                query_type.upper(),
                str(agg["count"]),
                format_val(agg["mean_ms"]),
                format_val(agg["median_ms"]),
                format_val(agg["std_ms"]),
            )

    console.print(table)


def print_pattern_table(query_type: str, by_pattern: Dict[str, dict]) -> None:
    table = Table(title=f"{query_type.upper()} by pattern")
    table.add_column("Pattern", style="bold")
    table.add_column("Mean (ms)", justify="right")
    table.add_column("Median (ms)", justify="right")

    for pt, pt_agg in by_pattern.items():
        if pt_agg.get("count", 0) > 0:
            table.add_row(
                pt,
                format_val(pt_agg["mean_ms"]),
                format_val(pt_agg["median_ms"]),
            )

    console.print(table)


def print_comparison_table(rows: List[dict]) -> None:
    table = Table(title="Performance comparison on BEAR-B-daily")
    table.add_column("System", style="bold")
    table.add_column("VM (ms)", justify="right")
    table.add_column("DM (ms)", justify="right")
    table.add_column("VQ (ms)", justify="right")
    table.add_column("Ingestion (s)", justify="right")
    table.add_column("Break-even (VM)", justify="right")

    for row in rows:
        style = "bold green" if row["system"] == "TAL (ours)" else None
        table.add_row(
            row["system"],
            format_val(row["vm_ms"]),
            format_val(row["dm_ms"]),
            format_val(row["vq_ms"]),
            format_val(row["ingestion_s"], ".0f"),
            format_val(row["break_even_vm"], ".0f") if row["break_even_vm"] else "---",
            style=style,
        )

    console.print(table)


def load_measured_ostrich_results(ostrich_results_file: Path) -> None:
    if not ostrich_results_file.exists():
        return
    console.print("[bold]Loading measured OSTRICH results[/bold]")
    with open(ostrich_results_file, "r", encoding="utf-8") as f:
        ostrich_data = json.load(f)
    results = ostrich_data["results"]
    measured: dict = {"source": "measured (this hardware)"}
    for qt in ["vm", "dm", "vq"]:
        qt_data = results.get(qt)
        if qt_data:
            by_pattern = qt_data.get("by_pattern", {})
            if by_pattern:
                measured[f"{qt}_ms"] = {pt: v["mean_ms"] for pt, v in by_pattern.items()}
            else:
                measured[f"{qt}_ms"] = {"avg": qt_data["mean_ms"]}
    ingestion_s = ostrich_data.get("ingestion_s")
    measured["ingestion_s"] = ingestion_s if ingestion_s else PUBLISHED_RESULTS["OSTRICH"]["ingestion_s"]
    measured["notes"] = "measured on this hardware"
    PUBLISHED_RESULTS["OSTRICH"] = measured


def load_tal_ingestion_time(ocdm_timing_file: Path, qlever_timing_file: Path) -> Optional[float]:
    total = 0.0
    found = False
    if ocdm_timing_file.exists():
        with open(ocdm_timing_file, "r", encoding="utf-8") as f:
            total += json.load(f)["ocdm_conversion_s"]
            found = True
    if qlever_timing_file.exists():
        with open(qlever_timing_file, "r", encoding="utf-8") as f:
            total += json.load(f)["qlever_indexing_s"]
            found = True
    return total if found else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--granularity", choices=["daily", "hourly", "instant"], default="daily")
    args = parser.parse_args()

    results_file = DATA_DIR / f"benchmark_results_{args.granularity}.json"
    ostrich_results_file = DATA_DIR / f"ostrich_benchmark_results_{args.granularity}.json"
    ocdm_timing_file = DATA_DIR / f"ocdm_conversion_time_{args.granularity}.json"
    qlever_timing_file = DATA_DIR / f"qlever_indexing_time_{args.granularity}.json"
    output_dir = DATA_DIR / "analysis" / args.granularity

    data = load_results(results_file)
    load_measured_ostrich_results(ostrich_results_file)

    tal_aggregates = {}
    for query_type in ["vm", "dm", "vq"]:
        results = data.get("results", {}).get(query_type, [])
        if results:
            agg = compute_aggregates(results)
            tal_aggregates[query_type] = agg
            by_pattern = compute_aggregates_by_pattern(results)
            print_pattern_table(query_type, by_pattern)

    console.print()
    print_results_table(tal_aggregates)

    console.rule("[bold]Generating output files")
    comparison = generate_comparison_table(tal_aggregates, ocdm_timing_file, qlever_timing_file)
    write_csv(comparison, output_dir / "comparison.csv")
    generate_latex_comparison(comparison, output_dir / "comparison.tex")

    console.print("\nGenerating capabilities table...")
    write_csv(generate_capabilities_table(), output_dir / "capabilities.csv")
    generate_latex_capabilities(output_dir / "capabilities.tex")

    summary = {
        "tal_aggregates": tal_aggregates,
        "comparison": comparison,
        "hardware": data.get("hardware", {}),
    }
    summary_path = output_dir / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)
    console.print(f"  Saved: {summary_path}")

    console.print()
    print_comparison_table(comparison)

    console.rule("[bold]Generating plots")
    ostrich_raw_files = [DATA_DIR / f"ostrich_raw_{pt}.txt" for pt in ["p", "po"]]
    plot_dir = output_dir / "plots"
    generate_plots(data, ostrich_raw_files, plot_dir)


if __name__ == "__main__":
    main()
