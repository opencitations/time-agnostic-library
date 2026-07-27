# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import csv
import json
import re
import statistics
from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import corpora
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from matplotlib.figure import Figure
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
    with filepath.open(encoding="utf-8") as f:
        return json.load(f)


def load_disk_usage(corpus_name: str) -> dict[str, int | None]:
    usage: dict[str, int | None] = {
        "ocdm_dataset_bytes": None,
        "ocdm_provenance_bytes": None,
        "virtuoso_store_bytes": None,
        "ostrich_store_bytes": None,
        "r43ples_store_bytes": None,
    }
    ocdm_file = DATA_DIR / f"ocdm_conversion_time_{corpus_name}.json"
    if ocdm_file.exists():
        with ocdm_file.open(encoding="utf-8") as f:
            data = json.load(f)
        usage["ocdm_dataset_bytes"] = data.get("dataset_bytes")
        usage["ocdm_provenance_bytes"] = data.get("provenance_bytes")
    virtuoso_file = DATA_DIR / f"virtuoso_ingestion_time_{corpus_name}.json"
    if virtuoso_file.exists():
        with virtuoso_file.open(encoding="utf-8") as f:
            data = json.load(f)
        usage["virtuoso_store_bytes"] = data.get("store_bytes")
    ostrich_file = DATA_DIR / f"ostrich_store_size_{corpus_name}.json"
    if ostrich_file.exists():
        with ostrich_file.open(encoding="utf-8") as f:
            data = json.load(f)
        usage["ostrich_store_bytes"] = data.get("store_bytes")
    r43ples_file = DATA_DIR / f"r43ples_ingestion_time_{corpus_name}.json"
    if r43ples_file.exists():
        with r43ples_file.open(encoding="utf-8") as f:
            data = json.load(f)
        usage["r43ples_store_bytes"] = data.get("store_bytes")
    return usage


def _format_bytes(b: int | None) -> str:
    if b is None:
        return "---"
    if b >= 1073741824:
        return f"{b / 1073741824:.2f} GB"
    if b >= 1048576:
        return f"{b / 1048576:.2f} MB"
    if b >= 1024:
        return f"{b / 1024:.2f} KB"
    return f"{b} B"


def print_disk_usage_table(usage: dict[str, int | None]) -> None:
    table = Table(title="Disk usage")
    table.add_column("Component", style="bold")
    table.add_column("Size", justify="right")
    table.add_column("Bytes", justify="right")

    ocdm_ds = usage["ocdm_dataset_bytes"]
    ocdm_prov = usage["ocdm_provenance_bytes"]
    ocdm_total = (ocdm_ds or 0) + (ocdm_prov or 0) if ocdm_ds is not None else None
    virtuoso = usage["virtuoso_store_bytes"]
    tal_total = (
        (ocdm_total or 0) + (virtuoso or 0)
        if ocdm_total is not None or virtuoso is not None
        else None
    )

    table.add_row("OCDM dataset", _format_bytes(ocdm_ds), str(ocdm_ds or "---"))
    table.add_row("OCDM provenance", _format_bytes(ocdm_prov), str(ocdm_prov or "---"))
    table.add_row("OCDM total", _format_bytes(ocdm_total), str(ocdm_total or "---"))
    table.add_row("Virtuoso store", _format_bytes(virtuoso), str(virtuoso or "---"))
    table.add_row(
        "TAL total (OCDM + Virtuoso)",
        _format_bytes(tal_total),
        str(tal_total or "---"),
        style="bold green",
    )
    table.add_row(
        "OSTRICH store",
        _format_bytes(usage["ostrich_store_bytes"]),
        str(usage["ostrich_store_bytes"] or "---"),
    )
    table.add_row(
        "R43ples store",
        _format_bytes(usage["r43ples_store_bytes"]),
        str(usage["r43ples_store_bytes"] or "---"),
    )

    console.print(table)


def compute_aggregates(results: list[dict]) -> dict:
    valid_means = [r["mean_s"] * 1000 for r in results if r["mean_s"] is not None]
    if not valid_means:
        return {"count": 0}
    agg: dict = {
        "count": len(results),
        "mean_ms": statistics.mean(valid_means),
        "median_ms": statistics.median(valid_means),
        "std_ms": statistics.stdev(valid_means) if len(valid_means) > 1 else 0.0,
        "min_ms": min(valid_means),
        "max_ms": max(valid_means),
    }
    valid_memory = [
        r["median_memory_bytes"]
        for r in results
        if r.get("median_memory_bytes") is not None
    ]
    if valid_memory:
        agg["mean_memory_bytes"] = statistics.mean(valid_memory)
        agg["median_memory_bytes"] = statistics.median(valid_memory)
        agg["max_memory_bytes"] = max(valid_memory)
    return agg


def compute_aggregates_by_pattern(results: list[dict]) -> dict[str, dict]:
    by_pattern = {}
    for r in results:
        pt = r.get("pattern_type", "unknown")
        by_pattern.setdefault(pt, []).append(r)
    return {pt: compute_aggregates(rs) for pt, rs in by_pattern.items()}


def compute_break_even(
    tal_mean_ms: float,
    other_mean_ms: float,
    ingestion_s: float,
) -> float | None:
    diff_ms = tal_mean_ms - other_mean_ms
    if diff_ms <= 0:
        return None
    ingestion_ms = ingestion_s * 1000
    return ingestion_ms / diff_ms


def _median_ms(
    entries: list[dict], key: str = "median_s", scale: float = 1000
) -> float:
    return statistics.median(r[key] * scale for r in entries if r[key] is not None)


def _group_by(entries: list[dict], field: str) -> dict:
    groups: dict = {}
    for r in entries:
        groups.setdefault(r[field], []).append(r)
    return groups


def load_tal_vm_by_version(
    vm_results: list[dict], pattern_filter: str | None = None
) -> dict[int, float]:
    filtered = [r for r in vm_results if r["median_s"] is not None]
    if pattern_filter:
        filtered = [r for r in filtered if r["pattern_type"] == pattern_filter]
    by_version = _group_by(filtered, "version_index")
    return {v: _median_ms(entries) for v, entries in sorted(by_version.items())}


def load_tal_dm_by_version(
    dm_results: list[dict], pattern_filter: str | None = None
) -> dict[int, float]:
    filtered = [r for r in dm_results if r["median_s"] is not None]
    if pattern_filter:
        filtered = [r for r in filtered if r["pattern_type"] == pattern_filter]
    by_end = _group_by(filtered, "version_end")
    return {v: _median_ms(entries) for v, entries in sorted(by_end.items())}


def load_tal_vq_median(
    vq_results: list[dict], pattern_filter: str | None = None
) -> float:
    filtered = [r for r in vq_results if r["median_s"] is not None]
    if pattern_filter:
        filtered = [r for r in filtered if r["pattern_type"] == pattern_filter]
    return _median_ms(filtered)


# --- OSTRICH raw file parsing ---


def _parse_ostrich_raw_files(raw_files: list[Path]) -> list[dict]:
    patterns = []
    for raw_file in raw_files:
        if not raw_file.exists():
            continue
        pattern_type = raw_file.stem.replace("ostrich_raw_", "").rsplit("_", 1)[0]
        current_pattern = None
        current_section = None
        for raw_line in raw_file.read_text().splitlines():
            line = raw_line.strip()
            match = re.match(r"---PATTERN START:\s*(.+)", line)
            if match:
                current_pattern = {
                    "pattern_type": pattern_type,
                    "vm": [],
                    "dm": [],
                    "vq": [],
                }
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
            if line.startswith(("---", "patch,", "patch_start,", "offset,")):
                continue
            if current_pattern is None or current_section is None:
                continue
            parts = line.split(",")
            if current_section == "vm" and len(parts) >= 7:
                current_pattern["vm"].append(
                    {"patch": int(parts[0]), "median_us": float(parts[4])}
                )
            elif current_section == "dm" and len(parts) >= 8:
                current_pattern["dm"].append(
                    {
                        "patch_start": int(parts[0]),
                        "patch_end": int(parts[1]),
                        "median_us": float(parts[5]),
                    }
                )
            elif current_section == "vq" and len(parts) >= 5:
                current_pattern["vq"].append({"median_us": float(parts[2])})
    return patterns


def _ostrich_median_ms_by_version(
    patterns: list[dict],
    query_type: str,
    version_field: str,
    pattern_filter: str | None = None,
    start_filter: int | None = None,
) -> dict[int, float]:
    by_version: dict[int, list] = {}
    for pat in patterns:
        if pattern_filter and pat["pattern_type"] != pattern_filter:
            continue
        for entry in pat[query_type]:
            if start_filter is not None and entry.get("patch_start") != start_filter:
                continue
            v = entry[version_field]
            by_version.setdefault(v, []).append(entry["median_us"] / 1000)
    return {v: statistics.median(vals) for v, vals in sorted(by_version.items())}


def _load_r43ples_results(r43ples_file: Path) -> dict | None:
    if not r43ples_file.exists():
        return None
    with r43ples_file.open(encoding="utf-8") as f:
        return json.load(f)


def load_r43ples_vm_by_version(
    r43ples_file: Path, pattern_filter: str | None = None
) -> dict[int, float]:
    data = _load_r43ples_results(r43ples_file)
    if not data:
        return {}
    by_version: dict[int, list] = {}
    for entry in data.get("detail", {}).get("per_version_vm", []):
        v = entry["version"]
        for pat in entry["patterns"]:
            if pattern_filter and pat["pattern_type"] != pattern_filter:
                continue
            by_version.setdefault(v, []).append(pat["median_ms"])
    return {v: statistics.median(vals) for v, vals in sorted(by_version.items())}


def load_r43ples_dm_by_version(
    r43ples_file: Path, pattern_filter: str | None = None
) -> dict[int, float]:
    data = _load_r43ples_results(r43ples_file)
    if not data:
        return {}
    by_version: dict[int, list] = {}
    for entry in data.get("detail", {}).get("per_delta_dm", []):
        v = entry["version_end"]
        for pat in entry["patterns"]:
            if pattern_filter and pat["pattern_type"] != pattern_filter:
                continue
            by_version.setdefault(v, []).append(pat["median_ms"])
    return {v: statistics.median(vals) for v, vals in sorted(by_version.items())}


def load_r43ples_vq_median(
    r43ples_file: Path, pattern_filter: str | None = None
) -> float | None:
    data = _load_r43ples_results(r43ples_file)
    if not data:
        return None
    entries = data.get("detail", {}).get("per_pattern_vq", [])
    if pattern_filter:
        entries = [e for e in entries if e["pattern_type"] == pattern_filter]
    if not entries:
        return None
    return statistics.median(e["median_ms"] for e in entries)


def load_ostrich_vm_by_version(
    raw_files: list[Path], pattern_filter: str | None = None
) -> dict[int, float]:
    patterns = _parse_ostrich_raw_files(raw_files)
    return _ostrich_median_ms_by_version(patterns, "vm", "patch", pattern_filter)


def load_ostrich_dm_by_version(
    raw_files: list[Path], pattern_filter: str | None = None
) -> dict[int, float]:
    patterns = _parse_ostrich_raw_files(raw_files)
    return _ostrich_median_ms_by_version(
        patterns, "dm", "patch_end", pattern_filter, start_filter=0
    )


def load_ostrich_vq_median(
    raw_files: list[Path], pattern_filter: str | None = None
) -> float:
    patterns = _parse_ostrich_raw_files(raw_files)
    medians = []
    for pat in patterns:
        if pattern_filter and pat["pattern_type"] != pattern_filter:
            continue
        medians.extend(entry["median_us"] / 1000 for entry in pat["vq"])
    return statistics.median(medians)


# --- Plotting ---


def _ms_formatter(val: float, _pos: int) -> str:
    if val >= 1:
        return f"{val:g}"
    return f"{val:.2g}"


def _format_log_axis(ax: Axes) -> None:
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(FuncFormatter(_ms_formatter))
    ax.yaxis.set_minor_formatter(NullFormatter())


def _save_plot(fig: Figure, plot_dir: Path, name: str) -> None:
    plot_dir.mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "jpg"):
        fig.savefig(plot_dir / f"{name}.{ext}", bbox_inches="tight", dpi=300)
    plt.close(fig)
    console.print(f"  Saved: {plot_dir / name}.{{pdf,jpg}}")


def generate_comparison_table(
    tal_results: dict, ocdm_timing_file: Path, virtuoso_timing_file: Path
) -> list[dict]:
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
                row["break_even_vm"] = compute_break_even(
                    tal_vm, row["vm_ms"], published["ingestion_s"]
                )
            tal_vq = tal_results.get("vq", {}).get("mean_ms")
            if tal_vq and row["vq_ms"]:
                row["break_even_vq"] = compute_break_even(
                    tal_vq, row["vq_ms"], published["ingestion_s"]
                )

        rows.append(row)

    tal_ingestion = load_tal_ingestion_time(ocdm_timing_file, virtuoso_timing_file)
    tal_row = {
        "system": "TAL (ours)",
        "source": "this work",
        "vm_ms": tal_results.get("vm", {}).get("mean_ms"),
        "dm_ms": tal_results.get("dm", {}).get("mean_ms"),
        "vq_ms": tal_results.get("vq", {}).get("mean_ms"),
        "ingestion_s": tal_ingestion or 0,
        "break_even_vm": None,
        "break_even_vq": None,
        "notes": "OCDM conversion + Virtuoso load + free-text index",
    }
    rows.append(tal_row)
    return rows


def generate_capabilities_table() -> list[dict]:
    return [
        {
            "system": "Jena-IC",
            "VM": "Y",
            "SV": "N",
            "CV": "N",
            "DM": "N",
            "SD": "N",
            "CD": "N",
        },
        {
            "system": "Jena-CB",
            "VM": "N",
            "SV": "N",
            "CV": "N",
            "DM": "Y",
            "SD": "N",
            "CD": "N",
        },
        {
            "system": "HDT-IC",
            "VM": "Y",
            "SV": "N",
            "CV": "N",
            "DM": "N",
            "SD": "N",
            "CD": "N",
        },
        {
            "system": "HDT-CB",
            "VM": "N",
            "SV": "N",
            "CV": "N",
            "DM": "Y",
            "SD": "N",
            "CD": "N",
        },
        {
            "system": "OSTRICH",
            "VM": "Y",
            "SV": "N",
            "CV": "N",
            "DM": "Y",
            "SD": "N",
            "CD": "N",
        },
        {
            "system": "R43ples",
            "VM": "Y",
            "SV": "N",
            "CV": "N",
            "DM": "N",
            "SD": "N",
            "CD": "N",
        },
        {
            "system": "v-RDFCSA",
            "VM": "Y",
            "SV": "N",
            "CV": "N",
            "DM": "Y",
            "SD": "N",
            "CD": "N",
        },
        {
            "system": "TrieDF",
            "VM": "Y",
            "SV": "N",
            "CV": "N",
            "DM": "Y",
            "SD": "N",
            "CD": "N",
        },
        {
            "system": "TAL (ours)",
            "VM": "Y",
            "SV": "Y",
            "CV": "Y",
            "DM": "Y",
            "SD": "Y",
            "CD": "Y",
        },
    ]


def write_csv(rows: list[dict], filepath: Path) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with filepath.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    console.print(f"  Saved: {filepath}")


def format_val(val, fmt=".2f") -> str:
    if val is None:
        return "---"
    return f"{val:{fmt}}"


def generate_latex_comparison(rows: list[dict], filepath: Path) -> None:
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
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\end{table}",
        ]
    )
    with filepath.open("w", encoding="utf-8") as f:
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
    lines.extend(
        [
            r"\bottomrule",
            r"\end{tabular}",
            r"\end{table}",
        ]
    )
    with filepath.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    console.print(f"  Saved: {filepath}")


def print_results_table(tal_aggregates: dict) -> None:
    table = Table(title="TAL benchmark results")
    table.add_column("Query type", style="bold")
    table.add_column("Count", justify="right")
    table.add_column("Mean (ms)", justify="right")
    table.add_column("Median (ms)", justify="right")
    table.add_column("Std (ms)", justify="right")
    table.add_column("Median mem (KB)", justify="right")

    for query_type in ["vm", "dm", "vq"]:
        agg = tal_aggregates.get(query_type)
        if agg and agg.get("count", 0) > 0:
            mem = agg.get("median_memory_bytes")
            mem_str = f"{mem / 1024:.0f}" if mem is not None else "---"
            table.add_row(
                query_type.upper(),
                str(agg["count"]),
                format_val(agg["mean_ms"]),
                format_val(agg["median_ms"]),
                format_val(agg["std_ms"]),
                mem_str,
            )

    console.print(table)


def print_pattern_table(query_type: str, by_pattern: dict[str, dict]) -> None:
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


def print_comparison_table(rows: list[dict]) -> None:
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


def load_measured_r43ples_results(r43ples_results_file: Path) -> None:
    if not r43ples_results_file.exists():
        return
    console.print("[bold]Loading measured R43ples results[/bold]")
    with r43ples_results_file.open(encoding="utf-8") as f:
        r43ples_data = json.load(f)
    results = r43ples_data["results"]
    measured: dict = {"source": "measured (this hardware)"}
    for qt in ["vm", "dm", "vq"]:
        qt_data = results.get(qt)
        if qt_data:
            by_pattern = qt_data.get("by_pattern", {})
            if by_pattern:
                measured[f"{qt}_ms"] = {
                    pt: v["mean_ms"] for pt, v in by_pattern.items()
                }
            else:
                measured[f"{qt}_ms"] = {"avg": qt_data["mean_ms"]}
    ingestion_s = r43ples_data.get("ingestion_s")
    measured["ingestion_s"] = ingestion_s
    measured["notes"] = "measured on this hardware"
    PUBLISHED_RESULTS["R43ples"] = measured


def load_measured_ostrich_results(ostrich_results_file: Path) -> None:
    if not ostrich_results_file.exists():
        return
    console.print("[bold]Loading measured OSTRICH results[/bold]")
    with ostrich_results_file.open(encoding="utf-8") as f:
        ostrich_data = json.load(f)
    results = ostrich_data["results"]
    measured: dict = {"source": "measured (this hardware)"}
    for qt in ["vm", "dm", "vq"]:
        qt_data = results.get(qt)
        if qt_data:
            by_pattern = qt_data.get("by_pattern", {})
            if by_pattern:
                measured[f"{qt}_ms"] = {
                    pt: v["mean_ms"] for pt, v in by_pattern.items()
                }
            else:
                measured[f"{qt}_ms"] = {"avg": qt_data["mean_ms"]}
    ingestion_s = ostrich_data.get("ingestion_s")
    measured["ingestion_s"] = ingestion_s or PUBLISHED_RESULTS["OSTRICH"]["ingestion_s"]
    measured["notes"] = "measured on this hardware"
    PUBLISHED_RESULTS["OSTRICH"] = measured


def load_tal_ingestion_time(
    ocdm_timing_file: Path, virtuoso_timing_file: Path
) -> float | None:
    """Time to go from the BEAR files to a queryable store.

    The free-text index is part of it.
    """
    total = 0.0
    found = False
    if ocdm_timing_file.exists():
        with ocdm_timing_file.open(encoding="utf-8") as f:
            total += json.load(f)["ocdm_conversion_s"]
            found = True
    if virtuoso_timing_file.exists():
        with virtuoso_timing_file.open(encoding="utf-8") as f:
            data = json.load(f)
        total += data["virtuoso_load_s"] + data["virtuoso_full_text_index_s"]
        found = True
    return total if found else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--corpus", choices=corpora.CORPUS_NAMES, default="bear-b-daily"
    )
    args = parser.parse_args()

    results_file = DATA_DIR / f"benchmark_results_{args.corpus}.json"
    ostrich_results_file = DATA_DIR / f"ostrich_benchmark_results_{args.corpus}.json"
    ocdm_timing_file = DATA_DIR / f"ocdm_conversion_time_{args.corpus}.json"
    virtuoso_timing_file = DATA_DIR / f"virtuoso_ingestion_time_{args.corpus}.json"
    output_dir = DATA_DIR / "analysis" / args.corpus

    r43ples_results_file = DATA_DIR / f"r43ples_benchmark_results_{args.corpus}.json"

    data = load_results(results_file)
    load_measured_ostrich_results(ostrich_results_file)
    load_measured_r43ples_results(r43ples_results_file)

    # Disk usage
    disk_usage = load_disk_usage(args.corpus)
    console.rule("[bold]Disk usage")
    print_disk_usage_table(disk_usage)

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
    comparison = generate_comparison_table(
        tal_aggregates, ocdm_timing_file, virtuoso_timing_file
    )
    write_csv(comparison, output_dir / "comparison.csv")
    generate_latex_comparison(comparison, output_dir / "comparison.tex")

    console.print("\nGenerating capabilities table...")
    write_csv(generate_capabilities_table(), output_dir / "capabilities.csv")
    generate_latex_capabilities(output_dir / "capabilities.tex")

    summary = {
        "tal_aggregates": tal_aggregates,
        "comparison": comparison,
        "hardware": data.get("hardware", {}),
        "disk_usage": disk_usage,
    }
    summary_path = output_dir / "summary.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)
    console.print(f"  Saved: {summary_path}")

    console.print()
    print_comparison_table(comparison)


if __name__ == "__main__":
    main()
