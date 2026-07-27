# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import gzip
import re
from collections import defaultdict
from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

PROV_NS = "http://www.w3.org/ns/prov#"
OCO_NS = "https://w3id.org/oc/ontology/"
DCTERMS_NS = "http://purl.org/dc/terms/"
XSD_NS = "http://www.w3.org/2001/XMLSchema#"

_TRIPLE_LEN = 3

# Regex to parse an N-Triples line into (subject, predicate, object) in a single
# C-level pass. Falls back to the character-by-character parser on mismatch.
#
# N-Triples line format:  <subject> <predicate> <object> .
#
# Group 1 - subject: URI <http://...> or blank node _:id
# Group 2 - predicate: always a URI <http://...>
# Group 3 - object, one of:
#   - URI:              <http://...>
#   - literal:          "text" optionally followed by @lang or ^^<datatype>
#   - blank node:       _:id
_NT_RE = re.compile(
    r"(<[^>]+>|_:\S+)\s+"  # group 1: subject (URI or blank node)
    r"(<[^>]+>)\s+"  # group 2: predicate (URI)
    r"(<[^>]+>"  # group 3 option a: URI object
    r'|"(?:[^"\\]|\\.)*"'  # group 3 option b: quoted literal (handles escapes)
    r"(?:@[a-zA-Z-]+|\^\^<[^>]+>)?"  # optional language tag or datatype
    r"|_:\S+)"  # group 3 option c: blank node object
    r"\s*\.\s*$"  # trailing dot and whitespace
)


def parse_ntriples_line(
    line: str,
    object_normalizer: Callable[[str], str] | None = None,
) -> tuple[str, str, str] | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    m = _NT_RE.match(line)
    if m:
        obj = m.group(3)
        if object_normalizer:
            obj = object_normalizer(obj)
        return (m.group(1), m.group(2), obj)
    if line.endswith(" ."):
        line = line[:-2]
    elif line.endswith("."):
        line = line[:-1]
    line = line.strip()
    parts = []
    i = 0
    while i < len(line) and len(parts) < _TRIPLE_LEN:
        if line[i] == "<":
            end = line.index(">", i)
            parts.append(line[i : end + 1])
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
            if rest_start < len(line) and line[rest_start : rest_start + 2] == "^^":
                dt_start = rest_start + 2
                if dt_start < len(line) and line[dt_start] == "<":
                    dt_end = line.index(">", dt_start)
                    parts.append(line[i : dt_end + 1])
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
                parts.append(line[i : end_quote + 1])
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
    if len(parts) == _TRIPLE_LEN:
        obj = parts[2]
        if object_normalizer:
            obj = object_normalizer(obj)
        return (parts[0], parts[1], obj)
    return None


def extract_subject_uri(s_term: str) -> str:
    if s_term.startswith("<") and s_term.endswith(">"):
        return s_term[1:-1]
    return s_term


def _open_ntriples(filepath: Path):
    if filepath.suffix == ".gz":
        return gzip.open(filepath, "rt", encoding="utf-8", errors="replace")
    return filepath.open(encoding="utf-8", errors="replace")


def _open_for_writing(filepath: Path):
    if filepath.suffix == ".gz":
        return gzip.open(filepath, "wt", encoding="utf-8", compresslevel=6)
    return filepath.open("w", encoding="utf-8")


def read_ntriples_file(
    filepath: Path,
    object_normalizer: Callable[[str], str] | None = None,
) -> list[tuple[str, str, str]]:
    triples = []
    with _open_ntriples(filepath) as f:
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


def _read_and_group(
    filepath: Path,
    object_normalizer: Callable[[str], str] | None = None,
) -> dict[str, set[tuple[str, str]]]:
    by_subject: dict[str, set[tuple[str, str]]] = defaultdict(set)
    match = _NT_RE.match
    with _open_ntriples(filepath) as f:
        for line in f:
            m = match(line)
            if m:
                s, p, obj = m.groups()
                if object_normalizer:
                    obj = object_normalizer(obj)
                uri = s[1:-1] if s[0] == "<" else s
            else:
                parsed = parse_ntriples_line(line, object_normalizer)
                if not parsed:
                    continue
                s, p, obj = parsed
                uri = s[1:-1] if s[0] == "<" and s[-1] == ">" else s
            by_subject[uri].add((p, obj))
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
    return escaped.replace("\t", "\\t")


class _ProvenanceWriter:
    """Serializes the provenance of every entity while the diffs are computed."""

    def __init__(
        self,
        out,
        data_graph_uri: str,
        agent_uri: str,
        timestamps: list[datetime],
    ) -> None:
        self._out = out
        self._data_graph_uri = data_graph_uri
        self._agent_uri = agent_uri
        self._timestamps = timestamps
        self._changes: dict[str, int] = {}
        # Entities whose last change left them without triples, and the version
        # it happened at. A reappearance removes the entry, so what is left at
        # the end are the entities to mark as invalidated.
        self._emptied_at: dict[str, int] = {}

    def created(self, entity_uri: str) -> None:
        """Record an entity the conversion has not seen before."""
        if entity_uri in self._changes:
            return
        self._changes[entity_uri] = 0
        prov_graph = f"<{entity_uri}/prov/>"
        se1_uri = f"<{entity_uri}/prov/se/1>"
        t0 = _format_timestamp(self._timestamps[0])
        self._out.write(
            f"{se1_uri} <{PROV_NS}specializationOf> <{entity_uri}> {prov_graph} .\n"
            f'{se1_uri} <{PROV_NS}generatedAtTime> "{t0}"^^<{XSD_NS}dateTime> '
            f"{prov_graph} .\n"
            f"{se1_uri} <{PROV_NS}wasAttributedTo> <{self._agent_uri}> {prov_graph} .\n"
            f'{se1_uri} <{DCTERMS_NS}description> "The entity has been created." '
            f"{prov_graph} .\n"
        )

    def changed(
        self,
        entity_uri: str,
        version_idx: int,
        deleted_po: set[tuple[str, str]],
        added_po: set[tuple[str, str]],
        *,
        emptied: bool,
    ) -> None:
        se_num = self._changes[entity_uri] + 2
        self._changes[entity_uri] += 1
        prov_graph = f"<{entity_uri}/prov/>"
        se_uri = f"<{entity_uri}/prov/se/{se_num}>"
        prev_se_uri = f"<{entity_uri}/prov/se/{se_num - 1}>"
        timestamp = _format_timestamp(self._timestamps[version_idx])
        update_query = _build_update_query(
            entity_uri, self._data_graph_uri, deleted_po, added_po
        )
        escaped_query = _escape_sparql_for_nquads(update_query)

        self._out.write(
            f"{se_uri} <{PROV_NS}specializationOf> <{entity_uri}> {prov_graph} .\n"
            f'{se_uri} <{PROV_NS}generatedAtTime> "{timestamp}"'
            f"^^<{XSD_NS}dateTime> {prov_graph} .\n"
            f"{se_uri} <{PROV_NS}wasAttributedTo> <{self._agent_uri}> {prov_graph} .\n"
            f'{se_uri} <{OCO_NS}hasUpdateQuery> "{escaped_query}" {prov_graph} .\n'
            f"{se_uri} <{DCTERMS_NS}description> "
            f'"The entity has been modified." {prov_graph} .\n'
            f"{se_uri} <{PROV_NS}wasDerivedFrom> {prev_se_uri} {prov_graph} .\n"
        )

        if emptied:
            self._emptied_at[entity_uri] = version_idx
        else:
            self._emptied_at.pop(entity_uri, None)

    def finish(self) -> None:
        """Mark as invalidated the entities that never came back."""
        for entity_uri, version_idx in self._emptied_at.items():
            se_num = self._changes[entity_uri] + 1
            self._out.write(
                f"<{entity_uri}/prov/se/{se_num}> <{PROV_NS}invalidatedAtTime> "
                f'"{_format_timestamp(self._timestamps[version_idx])}"'
                f"^^<{XSD_NS}dateTime> <{entity_uri}/prov/> .\n"
            )


class OCDMConverter:
    def __init__(
        self,
        data_graph_uri: str,
        agent_uri: str,
        object_normalizer: Callable[[str], str] | None = None,
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
        prev_by_subject: dict[str, set[tuple[str, str]]] = {}
        latest_by_subject: dict[str, set[tuple[str, str]]] = {}

        # Prefetch pipeline: a single background thread reads and parses the
        # next IC file while the main thread diffs the current pair. Two
        # consecutive versions are all a conversion ever holds.
        with (
            self._open_provenance(provenance_output, timestamps) as writer,
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            future = executor.submit(
                _read_and_group, ic_files[0], self.object_normalizer
            )

            for version_idx in range(len(ic_files)):
                cur_by_subject = future.result()

                if version_idx + 1 < len(ic_files):
                    future = executor.submit(
                        _read_and_group,
                        ic_files[version_idx + 1],
                        self.object_normalizer,
                    )

                for entity_uri in cur_by_subject:
                    writer.created(entity_uri)

                if version_idx > 0:
                    for entity_uri in prev_by_subject.keys() | cur_by_subject.keys():
                        prev_po = prev_by_subject.get(entity_uri, set())
                        cur_po = cur_by_subject.get(entity_uri, set())
                        deleted_po = prev_po - cur_po
                        added_po = cur_po - prev_po
                        if deleted_po or added_po:
                            writer.changed(
                                entity_uri,
                                version_idx,
                                deleted_po,
                                added_po,
                                emptied=not cur_po,
                            )

                prev_by_subject = cur_by_subject
                if version_idx == len(ic_files) - 1:
                    latest_by_subject = cur_by_subject

        self._write_dataset(dataset_output, latest_by_subject)

    def convert_from_cb(
        self,
        initial_snapshot: Path,
        changesets: list[tuple[Path, Path]],
        timestamps: list[datetime],
        dataset_output: Path,
        provenance_output: Path,
    ) -> None:
        current_state: dict[str, set[tuple[str, str]]] = defaultdict(
            set, _read_and_group(initial_snapshot, self.object_normalizer)
        )

        # Read the added and deleted files of each changeset in parallel.
        with (
            self._open_provenance(provenance_output, timestamps) as writer,
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            for entity_uri in current_state:
                writer.created(entity_uri)

            for changeset_idx, (added_file, deleted_file) in enumerate(changesets):
                version_idx = changeset_idx + 1

                fut_del = executor.submit(
                    _read_and_group, deleted_file, self.object_normalizer
                )
                fut_add = executor.submit(
                    _read_and_group, added_file, self.object_normalizer
                )
                deleted_by_subject = fut_del.result()
                added_by_subject = fut_add.result()

                for entity_uri in deleted_by_subject.keys() | added_by_subject.keys():
                    writer.created(entity_uri)
                    deleted_po = deleted_by_subject.get(entity_uri, set())
                    added_po = added_by_subject.get(entity_uri, set())

                    current_state[entity_uri] -= deleted_po
                    current_state[entity_uri] |= added_po

                    emptied = not current_state[entity_uri]
                    if emptied:
                        del current_state[entity_uri]

                    if deleted_po or added_po:
                        writer.changed(
                            entity_uri,
                            version_idx,
                            deleted_po,
                            added_po,
                            emptied=emptied,
                        )

        self._write_dataset(dataset_output, current_state)

    @contextmanager
    def _open_provenance(
        self, provenance_output: Path, timestamps: list[datetime]
    ) -> Generator[_ProvenanceWriter]:
        provenance_output.parent.mkdir(parents=True, exist_ok=True)
        with _open_for_writing(Path(provenance_output)) as out:
            writer = _ProvenanceWriter(
                out, self.data_graph_uri, self.agent_uri, timestamps
            )
            yield writer
            writer.finish()

    def _write_dataset(
        self, dataset_output: Path, latest_by_subject: dict[str, set[tuple[str, str]]]
    ) -> None:
        dataset_output.parent.mkdir(parents=True, exist_ok=True)
        with _open_for_writing(Path(dataset_output)) as out:
            for entity_uri in sorted(latest_by_subject):
                out.writelines(
                    f"<{entity_uri}> {p} {o} <{self.data_graph_uri}> .\n"
                    for p, o in sorted(latest_by_subject[entity_uri])
                )
