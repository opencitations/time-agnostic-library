import csv
import json
import statistics
from pathlib import Path
from typing import Dict, List, Optional

from rich.console import Console
from rich.table import Table

console = Console()

DATA_DIR = Path(__file__).parent / "data"
RESULTS_FILE = DATA_DIR / "benchmark_results.json"
OUTPUT_DIR = DATA_DIR / "analysis"

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
        "vm_ms": {"avg": 1.0},
        "dm_ms": {"avg": 1.0},
        "vq_ms": {"avg": 0.1},
        "ingestion_s": 3600,
        "notes": "Hours to days for BEAR-B-hourly (>3 days)",
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


def generate_comparison_table(tal_results: dict) -> List[dict]:
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

    tal_row = {
        "system": "TAL (ours)",
        "source": "this work",
        "vm_ms": tal_results.get("vm", {}).get("mean_ms"),
        "dm_ms": tal_results.get("dm", {}).get("mean_ms"),
        "vq_ms": tal_results.get("vq", {}).get("mean_ms"),
        "ingestion_s": 0,
        "break_even_vm": None,
        "break_even_vq": None,
        "notes": "No pre-indexing",
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


def main():
    data = load_results(RESULTS_FILE)

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
    comparison = generate_comparison_table(tal_aggregates)
    write_csv(comparison, OUTPUT_DIR / "comparison.csv")
    generate_latex_comparison(comparison, OUTPUT_DIR / "comparison.tex")

    console.print("\nGenerating capabilities table...")
    write_csv(generate_capabilities_table(), OUTPUT_DIR / "capabilities.csv")
    generate_latex_capabilities(OUTPUT_DIR / "capabilities.tex")

    summary = {
        "tal_aggregates": tal_aggregates,
        "comparison": comparison,
        "hardware": data.get("hardware", {}),
    }
    summary_path = OUTPUT_DIR / "summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)
    console.print(f"  Saved: {summary_path}")

    console.print()
    print_comparison_table(comparison)


if __name__ == "__main__":
    main()
