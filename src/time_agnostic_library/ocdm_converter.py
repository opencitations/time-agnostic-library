import gzip
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

PROV_NS = "http://www.w3.org/ns/prov#"
OCO_NS = "https://w3id.org/oc/ontology/"
DCTERMS_NS = "http://purl.org/dc/terms/"
XSD_NS = "http://www.w3.org/2001/XMLSchema#"


def parse_ntriples_line(
    line: str,
    object_normalizer: Optional[Callable[[str], str]] = None,
) -> Optional[tuple[str, str, str]]:
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
        obj = parts[2]
        if object_normalizer:
            obj = object_normalizer(obj)
        return (parts[0], parts[1], obj)
    return None


def extract_subject_uri(s_term: str) -> str:
    if s_term.startswith("<") and s_term.endswith(">"):
        return s_term[1:-1]
    return s_term


def read_ntriples_file(
    filepath: Path,
    object_normalizer: Optional[Callable[[str], str]] = None,
) -> list[tuple[str, str, str]]:
    triples = []
    if filepath.suffix == ".gz":
        opener = lambda: gzip.open(filepath, "rt", encoding="utf-8", errors="replace")
    else:
        opener = lambda: open(filepath, "r", encoding="utf-8", errors="replace")
    with opener() as f:
        for line in f:
            parsed = parse_ntriples_line(line, object_normalizer)
            if parsed:
                triples.append(parsed)
    return triples


def group_triples_by_subject(
    triples: list[tuple[str, str, str]],
) -> dict[str, set[tuple[str, str]]]:
    by_subject: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for s, p, o in triples:
        uri = extract_subject_uri(s)
        by_subject[uri].add((p, o))
    return by_subject


def _format_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _build_update_query(
    entity_uri: str,
    data_graph_uri: str,
    deleted_po: set[tuple[str, str]],
    added_po: set[tuple[str, str]],
) -> str:
    parts = []
    if deleted_po:
        triples = " ".join(f"<{entity_uri}> {p} {o} ." for p, o in deleted_po)
        parts.append(f"DELETE DATA {{ GRAPH <{data_graph_uri}> {{ {triples} }} }}")
    if added_po:
        triples = " ".join(f"<{entity_uri}> {p} {o} ." for p, o in added_po)
        parts.append(f"INSERT DATA {{ GRAPH <{data_graph_uri}> {{ {triples} }} }}")
    return "; ".join(parts)


def _escape_sparql_for_nquads(query: str) -> str:
    escaped = query.replace("\\", "\\\\")
    escaped = escaped.replace('"', '\\"')
    escaped = escaped.replace("\n", "\\n")
    escaped = escaped.replace("\r", "\\r")
    escaped = escaped.replace("\t", "\\t")
    return escaped


class OCDMConverter:
    def __init__(
        self,
        data_graph_uri: str,
        agent_uri: str,
        object_normalizer: Optional[Callable[[str], str]] = None,
    ):
        self.data_graph_uri = data_graph_uri
        self.agent_uri = agent_uri
        self.object_normalizer = object_normalizer

    def convert_from_ic(
        self,
        ic_files: list[Path],
        timestamps: list[datetime],
        dataset_output: Path,
        provenance_output: Path,
    ) -> None:
        all_entities: set[str] = set()
        entity_changes: dict[str, list[tuple[int, set[tuple[str, str]], set[tuple[str, str]]]]] = defaultdict(list)
        prev_by_subject: dict[str, set[tuple[str, str]]] = {}
        latest_by_subject: dict[str, set[tuple[str, str]]] = {}

        for version_idx, ic_file in enumerate(ic_files):
            triples = read_ntriples_file(ic_file, self.object_normalizer)
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
            if version_idx == len(ic_files) - 1:
                latest_by_subject = cur_by_subject

        self._write_ocdm_output(
            all_entities, entity_changes, latest_by_subject,
            timestamps, dataset_output, provenance_output,
        )

    def convert_from_cb(
        self,
        initial_snapshot: Path,
        changesets: list[tuple[Path, Path]],
        timestamps: list[datetime],
        dataset_output: Path,
        provenance_output: Path,
    ) -> None:
        all_entities: set[str] = set()
        entity_changes: dict[str, list[tuple[int, set[tuple[str, str]], set[tuple[str, str]]]]] = defaultdict(list)

        triples = read_ntriples_file(initial_snapshot, self.object_normalizer)
        current_state: dict[str, set[tuple[str, str]]] = defaultdict(set, group_triples_by_subject(triples))
        all_entities.update(current_state.keys())

        for changeset_idx, (added_file, deleted_file) in enumerate(changesets):
            version_idx = changeset_idx + 1

            deleted_triples = read_ntriples_file(deleted_file, self.object_normalizer)
            deleted_by_subject = group_triples_by_subject(deleted_triples)

            added_triples = read_ntriples_file(added_file, self.object_normalizer)
            added_by_subject = group_triples_by_subject(added_triples)

            changed_entities = set(deleted_by_subject.keys()) | set(added_by_subject.keys())
            all_entities.update(changed_entities)

            for entity_uri in changed_entities:
                deleted_po = deleted_by_subject.get(entity_uri, set())
                added_po = added_by_subject.get(entity_uri, set())

                current_state[entity_uri] -= deleted_po
                current_state[entity_uri] |= added_po

                if not current_state[entity_uri]:
                    del current_state[entity_uri]

                if deleted_po or added_po:
                    entity_changes[entity_uri].append((version_idx, deleted_po, added_po))

        self._write_ocdm_output(
            all_entities, entity_changes, current_state,
            timestamps, dataset_output, provenance_output,
        )

    def _write_ocdm_output(
        self,
        all_entities: set[str],
        entity_changes: dict[str, list[tuple[int, set[tuple[str, str]], set[tuple[str, str]]]]],
        latest_by_subject: dict[str, set[tuple[str, str]]],
        timestamps: list[datetime],
        dataset_output: Path,
        provenance_output: Path,
    ) -> None:
        dataset_output.parent.mkdir(parents=True, exist_ok=True)
        provenance_output.parent.mkdir(parents=True, exist_ok=True)
        sorted_entities = sorted(all_entities)

        with open(dataset_output, "w", encoding="utf-8") as f:
            for entity_uri in sorted_entities:
                po_set = latest_by_subject.get(entity_uri, set())
                for p, o in sorted(po_set):
                    f.write(f"<{entity_uri}> {p} {o} <{self.data_graph_uri}> .\n")

        with open(provenance_output, "w", encoding="utf-8") as f:
            for entity_uri in sorted_entities:
                prov_graph = f"<{entity_uri}/prov/>"
                changes = entity_changes.get(entity_uri, [])

                se1_uri = f"<{entity_uri}/prov/se/1>"
                t0 = _format_timestamp(timestamps[0])

                f.write(f'{se1_uri} <{PROV_NS}specializationOf> <{entity_uri}> {prov_graph} .\n')
                f.write(f'{se1_uri} <{PROV_NS}generatedAtTime> "{t0}"^^<{XSD_NS}dateTime> {prov_graph} .\n')
                f.write(f'{se1_uri} <{PROV_NS}wasAttributedTo> <{self.agent_uri}> {prov_graph} .\n')
                f.write(f'{se1_uri} <{DCTERMS_NS}description> "The entity has been created." {prov_graph} .\n')

                for change_idx, (version_idx, deleted_po, added_po) in enumerate(changes):
                    se_num = change_idx + 2
                    se_uri = f"<{entity_uri}/prov/se/{se_num}>"
                    timestamp = _format_timestamp(timestamps[version_idx])

                    f.write(f'{se_uri} <{PROV_NS}specializationOf> <{entity_uri}> {prov_graph} .\n')
                    f.write(f'{se_uri} <{PROV_NS}generatedAtTime> "{timestamp}"^^<{XSD_NS}dateTime> {prov_graph} .\n')
                    f.write(f'{se_uri} <{PROV_NS}wasAttributedTo> <{self.agent_uri}> {prov_graph} .\n')

                    update_query = _build_update_query(entity_uri, self.data_graph_uri, deleted_po, added_po)
                    escaped_query = _escape_sparql_for_nquads(update_query)
                    f.write(f'{se_uri} <{OCO_NS}hasUpdateQuery> "{escaped_query}" {prov_graph} .\n')
                    f.write(f'{se_uri} <{DCTERMS_NS}description> "The entity has been modified." {prov_graph} .\n')

                    prev_se_uri = f"<{entity_uri}/prov/se/{se_num - 1}>"
                    f.write(f'{se_uri} <{PROV_NS}wasDerivedFrom> {prev_se_uri} {prov_graph} .\n')

                    if change_idx == len(changes) - 1 and entity_uri not in latest_by_subject:
                        f.write(f'{se_uri} <{PROV_NS}invalidatedAtTime> "{timestamp}"^^<{XSD_NS}dateTime> {prov_graph} .\n')
