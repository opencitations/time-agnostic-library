import gzip
import json
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

console = Console()

DATA_GRAPH = "http://bear-benchmark.org/data/"
PROV_NS = "http://www.w3.org/ns/prov#"
OCO_NS = "https://w3id.org/oc/ontology/"
DCTERMS_NS = "http://purl.org/dc/terms/"
XSD_NS = "http://www.w3.org/2001/XMLSchema#"
AGENT_URI = "http://bear-benchmark.org/converter"

QLEVER_DATATYPE_NORMALIZATIONS = {
    f"^^<{XSD_NS}integer>": f"^^<{XSD_NS}int>",
}

BASE_TIMESTAMP = datetime(2015, 8, 1, 0, 0, 0, tzinfo=timezone.utc)
INTERVAL = timedelta(days=1)

DATA_DIR = Path(__file__).parent / "data" / "daily"

PROGRESS_COLUMNS = (
    SpinnerColumn(),
    TextColumn("[bold blue]{task.description}"),
    BarColumn(),
    MofNCompleteColumn(),
    TimeElapsedColumn(),
    TimeRemainingColumn(),
)


def normalize_object(obj: str) -> str:
    for old, new in QLEVER_DATATYPE_NORMALIZATIONS.items():
        if obj.endswith(old):
            return obj[:-len(old)] + new
    return obj


def parse_ntriples_line(line: str) -> Optional[Tuple[str, str, str]]:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if line.endswith(" ."):
        line = line[:-2]
    elif line.endswith("."):
        line = line[:-1]
    line = line.strip()
    parts = []
    i = 0
    while i < len(line) and len(parts) < 3:
        if line[i] == "<":
            end = line.index(">", i)
            parts.append(line[i:end + 1])
            i = end + 1
        elif line[i] == '"':
            j = i + 1
            while j < len(line):
                if line[j] == "\\" and j + 1 < len(line):
                    j += 2
                    continue
                if line[j] == '"':
                    break
                j += 1
            end_quote = j
            rest_start = end_quote + 1
            if rest_start < len(line) and line[rest_start:rest_start + 2] == "^^":
                dt_start = rest_start + 2
                if dt_start < len(line) and line[dt_start] == "<":
                    dt_end = line.index(">", dt_start)
                    parts.append(line[i:dt_end + 1])
                    i = dt_end + 1
                else:
                    space = line.find(" ", dt_start)
                    if space == -1:
                        parts.append(line[i:])
                        i = len(line)
                    else:
                        parts.append(line[i:space])
                        i = space
            elif rest_start < len(line) and line[rest_start] == "@":
                space = line.find(" ", rest_start)
                if space == -1:
                    parts.append(line[i:])
                    i = len(line)
                else:
                    parts.append(line[i:space])
                    i = space
            else:
                parts.append(line[i:end_quote + 1])
                i = end_quote + 1
        elif line[i] == "_":
            space = line.find(" ", i)
            if space == -1:
                parts.append(line[i:])
                i = len(line)
            else:
                parts.append(line[i:space])
                i = space
        elif line[i] == " " or line[i] == "\t":
            i += 1
        else:
            space = line.find(" ", i)
            if space == -1:
                parts.append(line[i:])
                i = len(line)
            else:
                parts.append(line[i:space])
                i = space
    if len(parts) == 3:
        return (parts[0], parts[1], normalize_object(parts[2]))
    return None


def extract_subject_uri(s_term: str) -> str:
    if s_term.startswith("<") and s_term.endswith(">"):
        return s_term[1:-1]
    return s_term


def read_ntriples_file(filepath: Path) -> List[Tuple[str, str, str]]:
    triples = []
    if filepath.suffix == ".gz":
        opener = lambda: gzip.open(filepath, "rt", encoding="utf-8", errors="replace")
    else:
        opener = lambda: open(filepath, "r", encoding="utf-8", errors="replace")
    with opener() as f:
        for line in f:
            parsed = parse_ntriples_line(line)
            if parsed:
                triples.append(parsed)
    return triples


def group_triples_by_subject(triples: List[Tuple[str, str, str]]) -> Dict[str, Set[Tuple[str, str]]]:
    by_subject = defaultdict(set)
    for s, p, o in triples:
        uri = extract_subject_uri(s)
        by_subject[uri].add((p, o))
    return by_subject


def find_ic_files(ic_dir: Path) -> List[Path]:
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


def format_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def build_update_query(
    entity_uri: str,
    deleted_po: Set[Tuple[str, str]],
    added_po: Set[Tuple[str, str]],
) -> str:
    parts = []
    if deleted_po:
        triples = " ".join(f"<{entity_uri}> {p} {o} ." for p, o in deleted_po)
        parts.append(f"DELETE DATA {{ GRAPH <{DATA_GRAPH}> {{ {triples} }} }}")
    if added_po:
        triples = " ".join(f"<{entity_uri}> {p} {o} ." for p, o in added_po)
        parts.append(f"INSERT DATA {{ GRAPH <{DATA_GRAPH}> {{ {triples} }} }}")
    return "; ".join(parts)


def escape_sparql_for_nquads(query: str) -> str:
    escaped = query.replace("\\", "\\\\")
    escaped = escaped.replace('"', '\\"')
    escaped = escaped.replace("\n", "\\n")
    escaped = escaped.replace("\r", "\\r")
    escaped = escaped.replace("\t", "\\t")
    return escaped


def convert_bear_to_ocdm(
    ic_dir: Path,
    output_dir: Path,
) -> Tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset_path = output_dir / "dataset.nq"
    provenance_path = output_dir / "provenance.nq"

    ic_files = find_ic_files(ic_dir)
    num_versions = len(ic_files)
    console.print(f"Found {num_versions} IC versions")

    console.print("Reading all IC files and computing diffs...")
    all_entities: Set[str] = set()
    entity_changes: Dict[str, List[Tuple[int, Set[Tuple[str, str]], Set[Tuple[str, str]]]]] = defaultdict(list)

    prev_by_subject: Dict[str, Set[Tuple[str, str]]] = {}
    latest_by_subject: Dict[str, Set[Tuple[str, str]]] = {}

    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task("Processing IC versions", total=num_versions)
        for version_idx, ic_file in enumerate(ic_files):
            triples = read_ntriples_file(ic_file)
            cur_by_subject = group_triples_by_subject(triples)
            all_entities.update(cur_by_subject.keys())

            if version_idx > 0:
                changed_entities = set(prev_by_subject.keys()) | set(cur_by_subject.keys())
                for entity_uri in changed_entities:
                    prev_po = prev_by_subject.get(entity_uri, set())
                    cur_po = cur_by_subject.get(entity_uri, set())
                    deleted_po = prev_po - cur_po
                    added_po = cur_po - prev_po
                    if deleted_po or added_po:
                        entity_changes[entity_uri].append((version_idx, deleted_po, added_po))

            prev_by_subject = cur_by_subject
            if version_idx == num_versions - 1:
                latest_by_subject = cur_by_subject
            progress.advance(task)

    sorted_entities = sorted(all_entities)

    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task("Writing dataset.nq", total=len(sorted_entities))
        with open(dataset_path, "w", encoding="utf-8") as f:
            for entity_uri in sorted_entities:
                po_set = latest_by_subject.get(entity_uri, set())
                for p, o in sorted(po_set):
                    f.write(f"<{entity_uri}> {p} {o} <{DATA_GRAPH}> .\n")
                progress.advance(task)

    with Progress(*PROGRESS_COLUMNS, console=console) as progress:
        task = progress.add_task("Writing provenance.nq", total=len(sorted_entities))
        with open(provenance_path, "w", encoding="utf-8") as f:
            for entity_uri in sorted_entities:
                prov_graph = f"<{entity_uri}/prov/>"
                changes = entity_changes.get(entity_uri, [])

                se1_uri = f"<{entity_uri}/prov/se/1>"
                t0 = format_timestamp(BASE_TIMESTAMP)

                f.write(f'{se1_uri} <{PROV_NS}specializationOf> <{entity_uri}> {prov_graph} .\n')
                f.write(f'{se1_uri} <{PROV_NS}generatedAtTime> "{t0}"^^<{XSD_NS}dateTime> {prov_graph} .\n')
                f.write(f'{se1_uri} <{PROV_NS}wasAttributedTo> <{AGENT_URI}> {prov_graph} .\n')
                f.write(f'{se1_uri} <{DCTERMS_NS}description> "The entity has been created." {prov_graph} .\n')

                last_state_empty = False
                for change_idx, (version_idx, deleted_po, added_po) in enumerate(changes):
                    se_num = change_idx + 2
                    se_uri = f"<{entity_uri}/prov/se/{se_num}>"
                    timestamp = format_timestamp(BASE_TIMESTAMP + INTERVAL * version_idx)

                    f.write(f'{se_uri} <{PROV_NS}specializationOf> <{entity_uri}> {prov_graph} .\n')
                    f.write(f'{se_uri} <{PROV_NS}generatedAtTime> "{timestamp}"^^<{XSD_NS}dateTime> {prov_graph} .\n')
                    f.write(f'{se_uri} <{PROV_NS}wasAttributedTo> <{AGENT_URI}> {prov_graph} .\n')

                    update_query = build_update_query(entity_uri, deleted_po, added_po)
                    escaped_query = escape_sparql_for_nquads(update_query)
                    f.write(f'{se_uri} <{OCO_NS}hasUpdateQuery> "{escaped_query}" {prov_graph} .\n')
                    f.write(f'{se_uri} <{DCTERMS_NS}description> "The entity has been modified." {prov_graph} .\n')

                    prev_se_uri = f"<{entity_uri}/prov/se/{se_num - 1}>"
                    f.write(f'{se_uri} <{PROV_NS}wasDerivedFrom> {prev_se_uri} {prov_graph} .\n')

                    if change_idx == len(changes) - 1 and entity_uri not in latest_by_subject:
                        last_state_empty = True
                        f.write(f'{se_uri} <{PROV_NS}invalidatedAtTime> "{timestamp}"^^<{XSD_NS}dateTime> {prov_graph} .\n')

                progress.advance(task)

    console.print(f"\nConversion complete:")
    console.print(f"  Entities: {len(all_entities)}")
    console.print(f"  Dataset: {dataset_path} ({dataset_path.stat().st_size / 1024:.1f} KB)")
    console.print(f"  Provenance: {provenance_path} ({provenance_path.stat().st_size / 1024:.1f} KB)")

    return dataset_path, provenance_path


def main():
    ic_dir = DATA_DIR / "IC"

    if not ic_dir.exists():
        raise FileNotFoundError(f"IC directory not found: {ic_dir}. Run download.py first.")

    start = time.perf_counter()
    convert_bear_to_ocdm(
        ic_dir=ic_dir,
        output_dir=DATA_DIR,
    )
    elapsed_s = time.perf_counter() - start

    timing_file = DATA_DIR.parent / "data" / "ocdm_conversion_time.json"
    timing_file.parent.mkdir(parents=True, exist_ok=True)
    with open(timing_file, "w", encoding="utf-8") as f:
        json.dump({"ocdm_conversion_s": round(elapsed_s, 2)}, f, indent=2)
    console.print(f"Conversion time: {elapsed_s:.2f}s (saved to {timing_file})")


if __name__ == "__main__":
    main()
