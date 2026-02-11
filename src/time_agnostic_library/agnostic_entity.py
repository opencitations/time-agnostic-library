#!/usr/bin/python
# Copyright (c) 2022-2025, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

import re
from datetime import datetime
from functools import lru_cache

from rdflib import Dataset

from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.sparql import Sparql, _n3_to_rdf_term, _n3_value
from time_agnostic_library.support import convert_to_datetime

_OPERATION_RE = re.compile(r'(DELETE|INSERT)\s+DATA', re.IGNORECASE)
_GRAPH_BLOCK_RE = re.compile(r'GRAPH\s*<([^>]+)>\s*\{', re.IGNORECASE)

_RDF_TERM_RE = re.compile(
    r'<([^>]+)>'
    r'|"((?:[^"\\]|\\.)*)"\^\^<([^>]+)>'
    r'|"((?:[^"\\]|\\.)*)"@([a-zA-Z][\w-]*)'
    r'|"((?:[^"\\]|\\.)*)"'
    r"|'((?:[^'\\]|\\.)*)'"
    r'|(_:\S+)',
    re.DOTALL,
)

_ESCAPE_CHAR_RE = re.compile(r'\\(.)')
_ESCAPE_CHAR_MAP = {'n': '\n', 'r': '\r', 't': '\t'}

_RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"


def _unescape_literal(s: str) -> str:
    if '\\' not in s:
        return s
    return _ESCAPE_CHAR_RE.sub(
        lambda m: _ESCAPE_CHAR_MAP.get(m.group(1), m.group(1)), s
    )


def _normalize_literal(raw: str) -> str:
    unescaped = _unescape_literal(raw)
    return unescaped.replace('\\', '\\\\').replace('"', '\\"')


def _regex_match_to_n3(match: re.Match) -> str:
    uri = match.group(1)
    if uri is not None:
        return f"<{uri}>"

    typed_value = match.group(2)
    if typed_value is not None:
        return f'"{_normalize_literal(typed_value)}"^^<{match.group(3)}>'

    lang_value = match.group(4)
    if lang_value is not None:
        return f'"{_normalize_literal(lang_value)}"@{match.group(5)}'

    double_quoted = match.group(6)
    if double_quoted is not None:
        return f'"{_normalize_literal(double_quoted)}"'

    single_quoted = match.group(7)
    if single_quoted is not None:
        return f'"{_normalize_literal(single_quoted)}"'

    return match.group(8)


_BRACE_OR_QUOTE_RE = re.compile(r'[{}\'"]')


def _find_matching_close_brace(text: str, start: int) -> int:
    pos = start
    length = len(text)
    depth = 1
    while pos < length:
        m = _BRACE_OR_QUOTE_RE.search(text, pos)
        if m is None:
            return length
        pos = m.start()
        char = text[pos]
        if char == '{':
            depth += 1
            pos += 1
        elif char == '}':
            depth -= 1
            if depth == 0:
                return pos
            pos += 1
        else:
            quote_char = char
            pos += 1
            while pos < length:
                q = text.find(quote_char, pos)
                if q == -1:
                    pos = length
                    break
                num_backslashes = 0
                check = q - 1
                while check >= start and text[check] == '\\':
                    num_backslashes += 1
                    check -= 1
                if num_backslashes % 2 == 0:
                    pos = q + 1
                    break
                pos = q + 1
    return length


def _fast_parse_update(update_query: str) -> list[tuple[str, list[tuple[str, str, str, str]]]]:
    operations: list[tuple[str, list[tuple[str, str, str, str]]]] = []
    operation_matches = list(_OPERATION_RE.finditer(update_query))

    for i, operation_match in enumerate(operation_matches):
        operation_type = 'DeleteData' if operation_match.group(1).upper() == 'DELETE' else 'InsertData'

        op_start = operation_match.end()
        op_end = operation_matches[i + 1].start() if i + 1 < len(operation_matches) else len(update_query)
        operation_body = update_query[op_start:op_end]

        quads: list[tuple[str, str, str, str]] = []
        for graph_match in _GRAPH_BLOCK_RE.finditer(operation_body):
            graph_n3 = f"<{graph_match.group(1)}>"
            triples_start = graph_match.end()
            triples_end = _find_matching_close_brace(operation_body, triples_start)
            triples_text = operation_body[triples_start:triples_end]

            term_matches = list(_RDF_TERM_RE.finditer(triples_text))
            for j in range(0, len(term_matches) - 2, 3):
                subject = _regex_match_to_n3(term_matches[j])
                predicate = _regex_match_to_n3(term_matches[j + 1])
                obj = _regex_match_to_n3(term_matches[j + 2])
                quads.append((subject, predicate, obj, graph_n3))

        operations.append((operation_type, quads))

    return operations


CONFIG_PATH = "./config.json"


@lru_cache(maxsize=4096)
def _parse_datetime(time_string: str) -> datetime:
    result = convert_to_datetime(time_string)
    assert isinstance(result, datetime)
    return result


def _quad_set_to_dataset(quads: set[tuple[str, ...]]) -> Dataset:
    ds = Dataset(default_union=True)
    for quad in quads:
        rdf_terms = tuple(_n3_to_rdf_term(t) for t in quad)
        ds.add(rdf_terms)  # type: ignore[arg-type]
    return ds


class AgnosticEntity:
    def __init__(self, res:str, config:dict, include_related_objects:bool=False, include_merged_entities:bool=False, include_reverse_relations:bool=False):
        self.res = res
        self.include_related_objects = include_related_objects
        self.include_merged_entities = include_merged_entities
        self.include_reverse_relations = include_reverse_relations
        self.config = config

    def get_history(self, include_prov_metadata: bool=False) -> tuple:
        if self.include_related_objects or self.include_merged_entities or self.include_reverse_relations:
            histories = {}
            self._collect_all_related_entities_histories(histories, include_prov_metadata)
            return self._get_merged_histories(histories, include_prov_metadata)
        else:
            entity_history = self._get_entity_current_state(include_prov_metadata)
            entity_history = self._get_old_graphs(entity_history)
            # Convert quad sets to Datasets at public API boundary
            converted = {}
            for uri, time_dict in entity_history[0].items():
                converted[uri] = {}
                for ts, quad_set in time_dict.items():
                    if quad_set is not None:
                        converted[uri][ts] = _quad_set_to_dataset(quad_set)
                    else:
                        converted[uri][ts] = Dataset(default_union=True)
            entity_history[0] = converted
            return tuple(entity_history)

    def _collect_all_related_entities_histories(
        self,
        histories: dict,
        include_prov_metadata: bool
    ) -> None:
        main_entity = AgnosticEntity(self.res, self.config, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
        entity_history = main_entity._get_entity_current_state(include_prov_metadata)
        entity_history = main_entity._get_old_graphs(entity_history)
        histories[self.res] = (entity_history[0], entity_history[1])

        processed_entities = {self.res}

        if self.include_related_objects:
            self._collect_related_objects_recursively(self.res, processed_entities, histories, include_prov_metadata)

        if self.include_merged_entities:
            self._collect_merged_entities_recursively(self.res, processed_entities, histories, include_prov_metadata)

        if self.include_reverse_relations:
            self._collect_reverse_relations_recursively(self.res, processed_entities, histories, include_prov_metadata)

    def _collect_related_objects_recursively(
        self,
        entity_uri: str,
        processed_entities: set[str],
        histories: dict,
        include_prov_metadata: bool,
        depth: int | None = None
    ) -> None:
        if depth is not None and depth <= 0:
            return

        next_depth = None if depth is None else depth - 1

        entity_graphs = histories[entity_uri][0][entity_uri] if entity_uri in histories else None
        if not entity_graphs:
            return

        prov_prefix = ProvEntity.PROV
        entity_n3 = f"<{entity_uri}>"
        rdf_type_n3 = f"<{_RDF_TYPE}>"
        related_objects = set()
        for quad_set in entity_graphs.values():
            if quad_set is None:
                continue
            for quad in quad_set:
                if quad[0] == entity_n3 and quad[2].startswith('<') and prov_prefix not in quad[1] and quad[1] != rdf_type_n3:
                    related_objects.add(_n3_value(quad[2]))

        for obj_uri in related_objects:
            if obj_uri not in processed_entities:
                processed_entities.add(obj_uri)
                agnostic_entity = AgnosticEntity(obj_uri, self.config, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
                entity_history = agnostic_entity._get_entity_current_state(include_prov_metadata)
                entity_history = agnostic_entity._get_old_graphs(entity_history)
                histories[obj_uri] = (entity_history[0], entity_history[1])
                self._collect_related_objects_recursively(obj_uri, processed_entities, histories, include_prov_metadata, next_depth)

    def _collect_merged_entities_recursively(
        self,
        entity_uri: str,
        processed_entities: set[str],
        histories: dict,
        include_prov_metadata: bool,
        depth: int | None = None
    ) -> None:
        if depth is not None and depth <= 0:
            return

        next_depth = None if depth is None else depth - 1

        merged_entities = self._find_merged_entities(entity_uri)

        for merged_entity_uri in merged_entities:
            if merged_entity_uri not in processed_entities:
                processed_entities.add(merged_entity_uri)
                agnostic_entity = AgnosticEntity(merged_entity_uri, self.config, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
                entity_history = agnostic_entity._get_entity_current_state(include_prov_metadata)
                entity_history = agnostic_entity._get_old_graphs(entity_history)
                histories[merged_entity_uri] = (entity_history[0], entity_history[1])
                self._collect_merged_entities_recursively(merged_entity_uri, processed_entities, histories, include_prov_metadata, next_depth)

    def _collect_reverse_relations_recursively(
        self,
        entity_uri: str,
        processed_entities: set[str],
        histories: dict,
        include_prov_metadata: bool,
        depth: int | None = None
    ) -> None:
        if depth is not None and depth <= 0:
            return

        next_depth = None if depth is None else depth - 1

        reverse_related_entities = self._find_reverse_related_entities(entity_uri)

        for reverse_entity_uri in reverse_related_entities:
            if reverse_entity_uri not in processed_entities:
                processed_entities.add(reverse_entity_uri)
                agnostic_entity = AgnosticEntity(reverse_entity_uri, self.config, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
                entity_history = agnostic_entity._get_entity_current_state(include_prov_metadata)
                entity_history = agnostic_entity._get_old_graphs(entity_history)
                histories[reverse_entity_uri] = (entity_history[0], entity_history[1])
                self._collect_reverse_relations_recursively(reverse_entity_uri, processed_entities, histories, include_prov_metadata, next_depth)

    def _get_merged_histories(
        self,
        histories: dict,
        include_prov_metadata: bool
    ) -> tuple:
        entity_histories = {}
        metadata = {}
        for entity_uri, (entity_history_dict, entity_metadata) in histories.items():
            entity_histories[entity_uri] = entity_history_dict[entity_uri]
            if include_prov_metadata and entity_metadata:
                metadata[entity_uri] = entity_metadata[entity_uri]

        main_entity_times = sorted(
            entity_histories[self.res].keys(), key=lambda x: _parse_datetime(x)
        )

        merged_histories = {self.res: {}}

        related_sorted_times = {}
        for entity_uri, entity_history in entity_histories.items():
            if entity_uri == self.res:
                continue
            related_sorted_times[entity_uri] = sorted(
                ((t, _parse_datetime(t)) for t in entity_history.keys()),
                key=lambda x: x[1]
            )

        for timestamp in main_entity_times:
            merged_set = set(entity_histories[self.res][timestamp])
            timestamp_dt = _parse_datetime(timestamp)

            for entity_uri, sorted_times in related_sorted_times.items():
                relevant_time = None
                for etime, etime_dt in sorted_times:
                    if etime_dt <= timestamp_dt:
                        relevant_time = etime
                    else:
                        break
                if relevant_time:
                    merged_set.update(entity_histories[entity_uri][relevant_time])

            merged_histories[self.res][timestamp] = _quad_set_to_dataset(merged_set)

        return merged_histories, metadata

    def get_state_at_time(
        self,
        time: tuple[str | None, str | None],
        include_prov_metadata: bool = False,
        ) -> tuple:
        if self.include_related_objects or self.include_merged_entities or self.include_reverse_relations:
            histories = {}
            self._collect_all_related_entities_states_at_time(histories, time, include_prov_metadata)
            return self._get_merged_histories_at_time(histories, include_prov_metadata)
        else:
            entity_graphs, entity_snapshots, other_snapshots_metadata = self._get_entity_state_at_time(time, include_prov_metadata)
            # Convert quad sets to Datasets at public API boundary
            converted = {}
            for ts, quad_set in entity_graphs.items():
                converted[ts] = _quad_set_to_dataset(quad_set)
            return converted, entity_snapshots, other_snapshots_metadata

    def _collect_all_related_entities_states_at_time(
        self,
        histories: dict,
        time: tuple[str | None, str | None],
        include_prov_metadata: bool
    ) -> None:
        main_entity = AgnosticEntity(self.res, self.config, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
        entity_graphs, entity_snapshots, other_snapshots_metadata = main_entity._get_entity_state_at_time(time, include_prov_metadata)
        histories[self.res] = (entity_graphs, entity_snapshots, other_snapshots_metadata)

        processed_entities = {self.res}

        if self.include_related_objects:
            self._collect_related_objects_states_at_time(self.res, processed_entities, histories, time, include_prov_metadata)

        if self.include_merged_entities:
            self._collect_merged_entities_states_at_time(self.res, processed_entities, histories, time, include_prov_metadata)

        if self.include_reverse_relations:
            self._collect_reverse_relations_states_at_time(self.res, processed_entities, histories, time, include_prov_metadata)

    def _collect_related_objects_states_at_time(
        self,
        entity_uri: str,
        processed_entities: set[str],
        histories: dict,
        time: tuple[str | None, str | None],
        include_prov_metadata: bool,
        depth: int | None = None
    ) -> None:
        if depth is not None and depth <= 0:
            return

        next_depth = None if depth is None else depth - 1

        entity_graphs = histories[entity_uri][0] if entity_uri in histories else None
        if not entity_graphs:
            return

        prov_prefix = ProvEntity.PROV
        entity_n3 = f"<{entity_uri}>"
        rdf_type_n3 = f"<{_RDF_TYPE}>"
        related_objects = set()
        for quad_set in entity_graphs.values():
            for quad in quad_set:
                if quad[0] == entity_n3 and quad[2].startswith('<') and prov_prefix not in quad[1] and quad[1] != rdf_type_n3:
                    related_objects.add(_n3_value(quad[2]))

        for obj_uri in related_objects:
            if obj_uri not in processed_entities:
                processed_entities.add(obj_uri)
                agnostic_entity = AgnosticEntity(obj_uri, self.config, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
                entity_graphs_new, entity_snapshots, other_snapshots_metadata = agnostic_entity._get_entity_state_at_time(time, include_prov_metadata)
                histories[obj_uri] = (entity_graphs_new, entity_snapshots, other_snapshots_metadata)
                self._collect_related_objects_states_at_time(obj_uri, processed_entities, histories, time, include_prov_metadata, next_depth)

    def _collect_merged_entities_states_at_time(
        self,
        entity_uri: str,
        processed_entities: set[str],
        histories: dict,
        time: tuple[str | None, str | None],
        include_prov_metadata: bool,
        depth: int | None = None
    ) -> None:
        if depth is not None and depth <= 0:
            return

        next_depth = None if depth is None else depth - 1

        merged_entities = self._find_merged_entities(entity_uri)

        for merged_entity_uri in merged_entities:
            if merged_entity_uri not in processed_entities:
                processed_entities.add(merged_entity_uri)
                agnostic_entity = AgnosticEntity(merged_entity_uri, self.config, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
                entity_graphs, entity_snapshots, other_snapshots_metadata = agnostic_entity._get_entity_state_at_time(time, include_prov_metadata)
                histories[merged_entity_uri] = (entity_graphs, entity_snapshots, other_snapshots_metadata)
                self._collect_merged_entities_states_at_time(merged_entity_uri, processed_entities, histories, time, include_prov_metadata, next_depth)

    def _collect_reverse_relations_states_at_time(
        self,
        entity_uri: str,
        processed_entities: set[str],
        histories: dict,
        time: tuple[str | None, str | None],
        include_prov_metadata: bool,
        depth: int | None = None
    ) -> None:
        if depth is not None and depth <= 0:
            return

        next_depth = None if depth is None else depth - 1

        reverse_related_entities = self._find_reverse_related_entities(entity_uri)

        for reverse_entity_uri in reverse_related_entities:
            if reverse_entity_uri not in processed_entities:
                processed_entities.add(reverse_entity_uri)
                agnostic_entity = AgnosticEntity(reverse_entity_uri, self.config, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
                entity_graphs, entity_snapshots, other_snapshots_metadata = agnostic_entity._get_entity_state_at_time(time, include_prov_metadata)
                histories[reverse_entity_uri] = (entity_graphs, entity_snapshots, other_snapshots_metadata)
                self._collect_reverse_relations_states_at_time(reverse_entity_uri, processed_entities, histories, time, include_prov_metadata, next_depth)

    def _get_merged_histories_at_time(
        self,
        histories: dict,
        include_prov_metadata: bool
    ) -> tuple:
        entity_histories = {}
        entity_snapshots_metadata = {}
        other_snapshots_metadata = {} if include_prov_metadata else None

        for entity_uri, (entity_graphs, entity_snapshots, other_snapshots) in histories.items():
            entity_histories[entity_uri] = entity_graphs
            entity_snapshots_metadata[entity_uri] = entity_snapshots
            if include_prov_metadata and other_snapshots and other_snapshots_metadata is not None:
                other_snapshots_metadata[entity_uri] = other_snapshots

        main_entity_times = sorted(
            set(entity_histories[self.res].keys()), key=lambda x: _parse_datetime(x)
        )

        merged_histories = {self.res: {}}

        related_sorted_times = {}
        for entity_uri, graphs_at_times in entity_histories.items():
            if entity_uri == self.res:
                continue
            related_sorted_times[entity_uri] = sorted(
                ((t, _parse_datetime(t)) for t in graphs_at_times.keys()),
                key=lambda x: x[1]
            )

        for timestamp in main_entity_times:
            merged_set = set(entity_histories[self.res][timestamp])
            timestamp_dt = _parse_datetime(timestamp)

            for entity_uri, sorted_times in related_sorted_times.items():
                graphs_at_times = entity_histories[entity_uri]
                if timestamp in graphs_at_times:
                    related_quads = graphs_at_times[timestamp]
                else:
                    relevant_time = None
                    for rt, rt_dt in sorted_times:
                        if rt_dt <= timestamp_dt:
                            relevant_time = rt
                        else:
                            break
                    if relevant_time:
                        related_quads = graphs_at_times[relevant_time]
                    else:
                        continue
                merged_set.update(related_quads)

            merged_histories[self.res][timestamp] = _quad_set_to_dataset(merged_set)

        return merged_histories, entity_snapshots_metadata, other_snapshots_metadata

    def _get_entity_state_at_time(
        self,
        time: tuple[str | None, str | None],
        include_prov_metadata: bool
    ) -> tuple:
        other_snapshots_metadata = {}
        is_quadstore = self.config["provenance"]["is_quadstore"]
        graph_statement = f"GRAPH <{self.res}/prov/>" if is_quadstore else ""
        if include_prov_metadata:
            query_snapshots = f"""
                SELECT ?snapshot ?time ?responsibleAgent ?updateQuery ?primarySource ?description ?invalidatedAtTime
                WHERE {{
                    {graph_statement}
                    {{
                        ?snapshot <{ProvEntity.iri_specialization_of}> <{self.res}>;
                            <{ProvEntity.iri_generated_at_time}> ?time;
                            <{ProvEntity.iri_was_attributed_to}> ?responsibleAgent.
                        OPTIONAL {{
                            ?snapshot <{ProvEntity.iri_invalidated_at_time}> ?invalidatedAtTime.
                        }}
                        OPTIONAL {{
                            ?snapshot <{ProvEntity.iri_description}> ?description.
                        }}
                        OPTIONAL {{
                            ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
                        }}
                        OPTIONAL {{
                            ?snapshot <{ProvEntity.iri_had_primary_source}> ?primarySource.
                        }}
                    }}
                }}
            """
        else:
            query_snapshots = f"""
                SELECT ?snapshot ?time ?updateQuery
                WHERE {{
                    {graph_statement}
                    {{
                        ?snapshot <{ProvEntity.iri_specialization_of}> <{self.res}>;
                            <{ProvEntity.iri_generated_at_time}> ?time.
                        OPTIONAL {{
                            ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
                        }}
                    }}
                }}
            """
        results = Sparql(query_snapshots, config=self.config).run_select_query()
        bindings = results['results']['bindings']
        if not bindings:
            return {}, {}, other_snapshots_metadata
        sorted_results = sorted(bindings, key=lambda x: _parse_datetime(x['time']['value']), reverse=True)
        relevant_results = _filter_timestamps_by_interval(time, sorted_results, time_index='time')
        if include_prov_metadata:
            relevant_snapshot_uris = {relevant_result['snapshot']['value'] for relevant_result in relevant_results}
            other_snapshots = [snapshot for snapshot in bindings if snapshot['snapshot']['value'] not in relevant_snapshot_uris]
            for other_snapshot in other_snapshots:
                snapshot_uri = other_snapshot['snapshot']['value']
                other_snapshots_metadata[snapshot_uri] = {
                    "generatedAtTime": other_snapshot['time']['value'],
                    "invalidatedAtTime": other_snapshot.get('invalidatedAtTime', {}).get('value'),
                    "wasAttributedTo": other_snapshot['responsibleAgent']['value'],
                    "hasUpdateQuery": other_snapshot.get('updateQuery', {}).get('value'),
                    "hadPrimarySource": other_snapshot.get('primarySource', {}).get('value'),
                    "description": other_snapshot.get('description', {}).get('value')
                }
        if not relevant_results:
            interval_start = _parse_datetime(time[0]) if time[0] else None
            if interval_start:
                earlier_snapshots = [r for r in bindings if _parse_datetime(r['time']['value']) <= interval_start]
                if earlier_snapshots:
                    latest_snapshot = max(earlier_snapshots, key=lambda x: _parse_datetime(x['time']['value']))
                    relevant_results = [latest_snapshot]
                else:
                    return {}, {}, other_snapshots_metadata
            else:
                return {}, {}, other_snapshots_metadata
        entity_snapshots = {}
        entity_graphs: dict[str, set[tuple[str, ...]]] = {}
        entity_quads = self._query_dataset(self.res)
        sorted_parsed = [(r, _parse_datetime(r['time']['value'])) for r in sorted_results]
        last_idx = len(relevant_results) - 1
        for i, relevant_result in enumerate(relevant_results):
            relevant_result_time = relevant_result['time']['value']
            relevant_result_dt = _parse_datetime(relevant_result_time)
            update_parts = [
                r['updateQuery']['value']
                for r, r_dt in sorted_parsed
                if 'updateQuery' in r and 'value' in r['updateQuery'] and r_dt > relevant_result_dt
            ]
            entity_present_graph = entity_quads if i == last_idx else set(entity_quads)
            if update_parts:
                self._manage_update_queries(entity_present_graph, ";".join(update_parts))
            timestamp_key = convert_to_datetime(relevant_result_time, stringify=True)
            entity_graphs[timestamp_key] = entity_present_graph  # type: ignore[index]
            if include_prov_metadata:
                snapshot_uri = relevant_result['snapshot']['value']
                entity_snapshots[snapshot_uri] = {
                    "generatedAtTime": relevant_result_time,
                    "invalidatedAtTime": relevant_result.get('invalidatedAtTime', {}).get('value'),
                    "wasAttributedTo": relevant_result['responsibleAgent']['value'],
                    "hasUpdateQuery": relevant_result.get('updateQuery', {}).get('value'),
                    "hadPrimarySource": relevant_result.get('primarySource', {}).get('value'),
                    "description": relevant_result.get('description', {}).get('value')
                }
        return entity_graphs, entity_snapshots, other_snapshots_metadata

    def _include_prov_metadata(self, triples_generated_at_time: list, current_state: set[tuple[str, ...]]) -> dict:
        res_n3 = f"<{self.res}>"
        rdf_type_n3 = f"<{_RDF_TYPE}>"
        entity_n3 = f"<{ProvEntity.iri_entity}>"
        for quad in current_state:
            if quad[0] == res_n3 and quad[1] == rdf_type_n3 and quad[2] == entity_n3:
                return {}
        prov_properties = {
            f"<{ProvEntity.iri_invalidated_at_time}>": "invalidatedAtTime",
            f"<{ProvEntity.iri_was_attributed_to}>": "wasAttributedTo",
            f"<{ProvEntity.iri_had_primary_source}>": "hadPrimarySource",
            f"<{ProvEntity.iri_description}>": "description",
            f"<{ProvEntity.iri_has_update_query}>": "hasUpdateQuery",
            f"<{ProvEntity.iri_was_derived_from}>": "wasDerivedFrom"
        }
        prov_metadata: dict = {
            self.res: {}
        }
        for triple in triples_generated_at_time:
            time = convert_to_datetime(_n3_value(triple[2]), stringify=True)
            snapshot_uri_str = _n3_value(triple[0])
            prov_metadata[self.res][snapshot_uri_str] = {
                "generatedAtTime": time,
                "invalidatedAtTime": None,
                "wasAttributedTo": None,
                "hadPrimarySource": None,
                "description": None,
                "hasUpdateQuery": None,
                "wasDerivedFrom": []
            }
        for metadata in dict(prov_metadata).values():
            for se_uri_str, snapshot_data in metadata.items():
                se_n3 = f"<{se_uri_str}>"
                for prov_prop_n3, abbr in prov_properties.items():
                    for quad in current_state:
                        if quad[0] == se_n3 and quad[1] == prov_prop_n3:
                            value = _n3_value(quad[2])
                            if abbr == "wasDerivedFrom":
                                snapshot_data[abbr].append(value)
                            else:
                                snapshot_data[abbr] = value

                if "wasDerivedFrom" in snapshot_data and isinstance(snapshot_data["wasDerivedFrom"], list):
                    snapshot_data["wasDerivedFrom"] = sorted(snapshot_data["wasDerivedFrom"])

        return prov_metadata

    def _get_entity_current_state(self, include_prov_metadata: bool = False) -> list:
        entity_current_state: list = [{self.res: {}}]
        current_state: set[tuple[str, ...]] = set()
        for quad in self._query_provenance(include_prov_metadata):
            current_state.add(quad)
        if len(current_state) == 0:
            entity_current_state.append({})
            return entity_current_state
        for quad in self._query_dataset(self.res):
            current_state.add(quad)
        gen_at_time_n3 = f"<{ProvEntity.iri_generated_at_time}>"
        triples_generated_at_time = [
            quad for quad in current_state if quad[1] == gen_at_time_n3
        ]
        most_recent_time = None
        most_recent_time_str: str | None = None
        for quad in triples_generated_at_time:
            snapshot_time_str = _n3_value(quad[2])
            snapshot_date_time = _parse_datetime(snapshot_time_str)
            if most_recent_time:
                if snapshot_date_time > most_recent_time:
                    most_recent_time = snapshot_date_time
                    most_recent_time_str = snapshot_time_str
            else:
                most_recent_time = snapshot_date_time
                most_recent_time_str = snapshot_time_str
            entity_current_state[0][self.res][snapshot_time_str] = None
        entity_current_state[0][self.res][most_recent_time_str] = current_state
        if include_prov_metadata:
            prov_metadata = self._include_prov_metadata(
                triples_generated_at_time, current_state
            )
            entity_current_state.append(prov_metadata)
        else:
            entity_current_state.append(None)
        return entity_current_state

    def _get_old_graphs(self, entity_current_state: list) -> list:
        ordered_data: list[tuple[str, set[tuple[str, ...]]]] = sorted(
            entity_current_state[0][self.res].items(),
            key=lambda x: _parse_datetime(str(x[0])),
            reverse=True
        )
        if not ordered_data:
            return entity_current_state
        most_recent_graph = ordered_data[0][1]
        gen_at_time_n3 = f"<{ProvEntity.iri_generated_at_time}>"
        has_uq_n3 = f"<{ProvEntity.iri_has_update_query}>"
        snapshot_update_queries: dict[str, str | None] = {}
        for quad in most_recent_graph:
            if quad[1] == gen_at_time_n3:
                time_val = _n3_value(quad[2])
                uq_val = None
                for q2 in most_recent_graph:
                    if q2[0] == quad[0] and q2[1] == has_uq_n3:
                        uq_val = _n3_value(q2[2])
                        break
                snapshot_update_queries[time_val] = uq_val
        for index, date_graph in enumerate(ordered_data):
            if index > 0:
                next_snapshot = ordered_data[index-1][0]
                previous_graph = set(entity_current_state[0][self.res][next_snapshot])
                update_query = snapshot_update_queries.get(str(next_snapshot))
                if update_query is None:
                    entity_current_state[0][self.res][date_graph[0]] = previous_graph
                else:
                    self._manage_update_queries(previous_graph, update_query)
                    entity_current_state[0][self.res][date_graph[0]] = previous_graph
        spec_of_n3 = f"<{ProvEntity.iri_specialization_of}>"
        res_n3 = f"<{self.res}>"
        for time in list(entity_current_state[0][self.res]):
            cg_no_pro = entity_current_state[0][self.res].pop(time)
            prov_entities = set()
            for quad in cg_no_pro:
                if quad[1] == spec_of_n3 and quad[2] == res_n3:
                    prov_entities.add(quad[0])

            to_remove = set()
            for prov_entity_n3 in prov_entities:
                for quad in cg_no_pro:
                    if quad[0] == prov_entity_n3:
                        to_remove.add(quad)
            cg_no_pro -= to_remove

            time_str = str(convert_to_datetime(str(time), stringify=True))
            entity_current_state[0][self.res][time_str] = cg_no_pro
        return entity_current_state

    def iter_versions(self):
        prov_quads = self._query_provenance(include_prov_metadata=False)
        if len(prov_quads) == 0:
            return
        dataset_quads = self._query_dataset(self.res)
        working: set[tuple[str, ...]] = set()
        working.update(prov_quads)
        working.update(dataset_quads)
        gen_at_time_n3 = f"<{ProvEntity.iri_generated_at_time}>"
        has_uq_n3 = f"<{ProvEntity.iri_has_update_query}>"
        snapshots: dict[str, str | None] = {}
        for quad in prov_quads:
            if quad[1] == gen_at_time_n3:
                time_str = _n3_value(quad[2])
                uq_val = None
                for q2 in prov_quads:
                    if q2[0] == quad[0] and q2[1] == has_uq_n3:
                        uq_val = _n3_value(q2[2])
                        break
                snapshots[time_str] = uq_val
        ordered = sorted(snapshots.items(), key=lambda x: _parse_datetime(x[0]), reverse=True)
        for i, (time_str, update_query) in enumerate(ordered):
            if i > 0:
                prev_update = ordered[i - 1][1]
                if prev_update is not None:
                    self._manage_update_queries(working, prev_update)
            normalized = str(convert_to_datetime(time_str, stringify=True))
            yield normalized, _quad_set_to_dataset(working)

    @classmethod
    def _manage_update_queries(cls, graph: set, update_query: str) -> None:
        operations = _fast_parse_update(update_query)
        for operation_type, quads in operations:
            if operation_type == 'DeleteData':
                for quad in quads:
                    graph.add(quad)
            elif operation_type == 'InsertData':
                for quad in quads:
                    graph.discard(quad)

    def _query_dataset(self, entity_uri: str | None = None) -> set[tuple[str, ...]]:
        entity_uri = self.res if entity_uri is None else entity_uri

        is_quadstore = self.config['dataset']['is_quadstore']

        if is_quadstore:
            query_dataset = f"""
                SELECT ?s ?p ?o ?g
                WHERE {{
                    GRAPH ?g {{
                        VALUES ?s {{<{entity_uri}>}}
                        ?s ?p ?o
                    }}
                }}
            """
        else:
            query_dataset = f"""
                SELECT ?s ?p ?o
                WHERE {{
                    VALUES ?s {{<{entity_uri}>}}
                    ?s ?p ?o
                }}
            """

        return Sparql(query_dataset, config=self.config).run_select_to_quad_set()

    def _query_provenance(self, include_prov_metadata:bool=False) -> set[tuple[str, ...]]:
        if include_prov_metadata:
            query_provenance = f"""
                SELECT ?s ?p ?o WHERE {{
                    ?s <{ProvEntity.iri_specialization_of}> <{self.res}>;
                       <{ProvEntity.iri_was_attributed_to}> ?_agent;
                       <{ProvEntity.iri_generated_at_time}> ?_t;
                       <{ProvEntity.iri_description}> ?_desc.
                    ?s ?p ?o.
                    VALUES ?p {{
                        <{ProvEntity.iri_generated_at_time}>
                        <{ProvEntity.iri_was_attributed_to}>
                        <{ProvEntity.iri_had_primary_source}>
                        <{ProvEntity.iri_description}>
                        <{ProvEntity.iri_has_update_query}>
                        <{ProvEntity.iri_invalidated_at_time}>
                        <{ProvEntity.iri_was_derived_from}>
                        <{ProvEntity.iri_specialization_of}>
                    }}
                }}
            """
        else:
            query_provenance = f"""
                SELECT ?s ?p ?o WHERE {{
                    ?s <{ProvEntity.iri_specialization_of}> <{self.res}>;
                       <{ProvEntity.iri_generated_at_time}> ?_t.
                    ?s ?p ?o.
                    VALUES ?p {{
                        <{ProvEntity.iri_generated_at_time}>
                        <{ProvEntity.iri_has_update_query}>
                        <{ProvEntity.iri_was_derived_from}>
                        <{ProvEntity.iri_specialization_of}>
                    }}
                }}
            """
        return Sparql(query_provenance, config=self.config).run_select_to_quad_set()

    def _find_merged_entities(self, entity_uri: str) -> set[str]:
        merged_entity_uris = set()
        query_simple = f"""
            SELECT ?merged_entity_uri
            WHERE {{
                ?snapshot <{ProvEntity.iri_specialization_of}> <{entity_uri}> .
                ?snapshot <{ProvEntity.iri_was_derived_from}> ?derived_snapshot .
                ?derived_snapshot <{ProvEntity.iri_specialization_of}> ?merged_entity_uri .
                FILTER (?merged_entity_uri != <{entity_uri}>)
            }}
        """
        try:
            results = Sparql(query_simple, config=self.config).run_select_query()
            bindings = results.get('results', {}).get('bindings', [])
            for binding in bindings:
                if 'merged_entity_uri' in binding and 'value' in binding['merged_entity_uri']:
                    merged_entity_uris.add(binding['merged_entity_uri']['value'])
        except Exception as e:
            print(f"Error querying for merged entities for {entity_uri}: {e}")

        return merged_entity_uris

    def _find_reverse_related_entities(self, entity_uri: str) -> set[str]:
        reverse_related_entity_uris = set()

        is_quadstore = self.config['dataset']['is_quadstore']

        if is_quadstore:
            query = f"""
                SELECT ?subject
                WHERE {{
                    GRAPH ?g {{
                        ?subject ?predicate <{entity_uri}> .
                        FILTER(?predicate != <{_RDF_TYPE}> && !strstarts(str(?predicate), "{ProvEntity.PROV}"))
                    }}
                }}
            """
        else:
            query = f"""
                SELECT ?subject
                WHERE {{
                    ?subject ?predicate <{entity_uri}> .
                    FILTER(?predicate != <{_RDF_TYPE}> && !strstarts(str(?predicate), "{ProvEntity.PROV}"))
                }}
            """

        try:
            results = Sparql(query, config=self.config).run_select_query()
            bindings = results.get('results', {}).get('bindings', [])
            for binding in bindings:
                if 'subject' in binding and 'value' in binding['subject']:
                    subject_uri = binding['subject']['value']
                    if subject_uri != entity_uri:
                        reverse_related_entity_uris.add(subject_uri)
        except Exception as e:
            print(f"Error querying for reverse related entities for {entity_uri}: {e}")

        return reverse_related_entity_uris

def _filter_timestamps_by_interval(interval: tuple[str | None, str | None] | None, iterator: list, time_index: str | None = None) -> list:
    if interval:
        after_time = _parse_datetime(interval[0]) if interval[0] else None
        before_time = _parse_datetime(interval[1]) if interval[1] else None
        relevant_timestamps = []
        for timestamp in iterator:
            if time_index is not None and time_index in timestamp:
                time_binding = timestamp[time_index]
                if 'value' in time_binding:
                    time_str = time_binding['value']
                    time = _parse_datetime(time_str)
                else:
                    continue
            else:
                continue
            if after_time and before_time:
                if after_time <= time <= before_time:
                    relevant_timestamps.append(timestamp)
            elif after_time and not before_time:
                if time >= after_time:
                    relevant_timestamps.append(timestamp)
            elif before_time and not after_time:
                if time <= before_time:
                    relevant_timestamps.append(timestamp)
            else:
                relevant_timestamps.append(timestamp)
    else:
        relevant_timestamps = iterator.copy()
    return relevant_timestamps
