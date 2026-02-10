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


import json
import multiprocessing
import os
import sys
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

from rdflib import ConjunctiveGraph, Dataset, Graph, Literal, URIRef, Variable
from rdflib.paths import InvPath
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib.plugins.sparql.processor import prepareQuery

from time_agnostic_library.agnostic_entity import (
    AgnosticEntity,
    _fast_parse_update,
    _filter_timestamps_by_interval,
    _parse_datetime,
)
from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.sparql import Sparql
from time_agnostic_library.support import convert_to_datetime

CONFIG_PATH = "./config.json"

_MP_CONTEXT = multiprocessing.get_context('fork') if sys.platform != 'win32' else None
_PARALLEL_THRESHOLD = os.cpu_count() or 1


def _create_executor(max_workers=None):
    if _MP_CONTEXT is not None:
        return ProcessPoolExecutor(max_workers=max_workers, mp_context=_MP_CONTEXT)
    return ThreadPoolExecutor(max_workers=max_workers)


def _run_in_parallel(worker_fn, args_list):
    if len(args_list) < _PARALLEL_THRESHOLD:
        for args in args_list:
            yield worker_fn(*args)
        return
    with _create_executor() as executor:
        futures = {executor.submit(worker_fn, *args): i for i, args in enumerate(args_list)}
        for future in as_completed(futures):
            yield future.result()


def _reconstruct_entity_worker(entity, config, on_time, other_snapshots_flag):
    agnostic_entity = AgnosticEntity(
        entity, config=config,
        include_related_objects=False,
        include_merged_entities=False,
        include_reverse_relations=False,
    )
    if on_time:
        entity_graphs, _, other_snapshots = agnostic_entity.get_state_at_time(
            time=on_time, include_prov_metadata=other_snapshots_flag,
        )
        return entity, entity_graphs, other_snapshots
    entity_history = agnostic_entity.get_history(include_prov_metadata=True)
    return entity, entity_history[0], {}


def _sparql_values(uris: set[str]) -> str:
    return " ".join(f"<{uri}>" for uri in uris)


def _wrap_in_graph(body: str, is_quadstore: bool) -> str:
    if is_quadstore:
        return f"GRAPH ?g {{ {body} }}"
    return body


def _batch_query_provenance_snapshots(entity_uris: set[str], config: dict) -> dict[str, list[dict]]:
    values = _sparql_values(entity_uris)
    body = f"""
        ?snapshot <{ProvEntity.iri_specialization_of}> ?entity;
            <{ProvEntity.iri_generated_at_time}> ?time.
        OPTIONAL {{ ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery. }}
        VALUES ?entity {{ {values} }}
    """
    wrapped = _wrap_in_graph(body, config["provenance"]["is_quadstore"])
    query = f"SELECT ?entity ?time ?updateQuery WHERE {{ {wrapped} }}"
    results = Sparql(query, config).run_select_query()
    output: dict[str, list[dict]] = {uri: [] for uri in entity_uris}
    for binding in results['results']['bindings']:
        entity_uri = binding['entity']['value']
        entry = {
            'time': binding['time']['value'],
            'updateQuery': binding['updateQuery']['value'] if 'updateQuery' in binding else None,
        }
        output[entity_uri].append(entry)
    return output


def _batch_query_dataset_triples(entity_uris: set[str], config: dict) -> dict[str, set[tuple]]:
    values = _sparql_values(entity_uris)
    is_quadstore = config['dataset']['is_quadstore']
    body = f"VALUES ?s {{ {values} }} ?s ?p ?o"
    wrapped = _wrap_in_graph(body, is_quadstore)
    select_vars = "?s ?p ?o ?g" if is_quadstore else "?s ?p ?o"
    query = f"SELECT {select_vars} WHERE {{ {wrapped} }}"
    results = Sparql(query, config).run_select_query()
    output: dict[str, set[tuple]] = {uri: set() for uri in entity_uris}
    to_term = Sparql.binding_to_rdf_term
    for binding in results['results']['bindings']:
        s_val = binding['s']['value']
        s = URIRef(s_val)
        p = URIRef(binding['p']['value'])
        o = to_term(binding['o'])
        if is_quadstore and 'g' in binding:
            output[s_val].add((s, p, o, URIRef(binding['g']['value'])))
        else:
            output[s_val].add((s, p, o))
    return output


def _reconstruct_at_time_from_data(
    prov_snapshots: list[dict],
    dataset_quads: set[tuple],
    on_time: tuple[str | None, str | None],
) -> dict[str, Dataset]:
    if not prov_snapshots:
        return {}
    sorted_snaps = sorted(prov_snapshots, key=lambda x: _parse_datetime(x['time']), reverse=True)
    relevant = _filter_timestamps_by_interval(
        on_time,
        [{'time': {'value': s['time']}} for s in sorted_snaps],
        time_index='time',
    )
    if not relevant:
        interval_start = _parse_datetime(on_time[0]) if on_time[0] else None
        if interval_start:
            earlier = [s for s in sorted_snaps if _parse_datetime(s['time']) <= interval_start]
            if earlier:
                best = max(earlier, key=lambda x: _parse_datetime(x['time']))
                relevant = [{'time': {'value': best['time']}}]
            else:
                return {}
        else:
            return {}
    sorted_parsed = [(s, _parse_datetime(s['time'])) for s in sorted_snaps]
    result: dict[str, Dataset] = {}
    for rel in relevant:
        rel_time = rel['time']['value']
        rel_dt = _parse_datetime(rel_time)
        update_parts = [
            s['updateQuery'] for s, s_dt in sorted_parsed
            if s['updateQuery'] is not None and s_dt > rel_dt
        ]
        cg = Dataset(default_union=True)
        for quad in dataset_quads:
            cg.add(quad)
        if update_parts:
            AgnosticEntity._manage_update_queries(cg, ";".join(update_parts))
        ts_key = str(convert_to_datetime(rel_time, stringify=True))
        result[ts_key] = cg
    return result


def _iter_versions_as_sets(
    prov_snapshots: list[dict],
    dataset_quads: set[tuple],
) -> list[tuple[str, tuple]]:
    if not prov_snapshots:
        return []
    sorted_snaps = sorted(prov_snapshots, key=lambda x: _parse_datetime(x['time']), reverse=True)
    working = set(dataset_quads)
    results = []
    for i, snap in enumerate(sorted_snaps):
        if i > 0:
            prev_uq = sorted_snaps[i - 1]['updateQuery']
            if prev_uq is not None:
                for op_type, quads in _fast_parse_update(prev_uq):
                    if op_type == 'DeleteData':
                        for quad in quads:
                            working.add(quad)
                    elif op_type == 'InsertData':
                        for quad in quads:
                            working.discard(quad)
        normalized = str(convert_to_datetime(snap['time'], stringify=True))
        results.append((normalized, tuple(working)))
    return results


def _match_single_pattern(triple_pattern: tuple, quads: tuple) -> list[dict]:
    # Replaces rdflib's graph.query() for single triple patterns like "?s <pred> ?o".
    # Each position in the pattern is either a concrete term (URIRef/Literal) or a Variable.
    s_pat, p_pat, o_pat = triple_pattern[0], triple_pattern[1], triple_pattern[2]
    s_is_var = isinstance(s_pat, Variable)
    p_is_var = isinstance(p_pat, Variable)
    o_is_var = isinstance(o_pat, Variable)
    bindings = []
    for quad in quads:
        s, p, o = quad[0], quad[1], quad[2]
        if not p_is_var and p != p_pat:
            continue
        if not o_is_var and o != o_pat:
            continue
        # Build a SPARQL JSON binding for each variable position
        binding = {}
        if s_is_var:
            binding[str(s_pat)] = Sparql._format_result_value(s)
        if p_is_var:
            binding[str(p_pat)] = Sparql._format_result_value(p)
        if o_is_var:
            binding[str(o_pat)] = Sparql._format_result_value(o)
        bindings.append(binding)
    return bindings


def _batch_query_dm_provenance(entity_uris: set[str], config: dict) -> dict[str, list[dict]]:
    values = _sparql_values(entity_uris)
    query = f"""
        SELECT ?entity ?time ?updateQuery ?description
        WHERE {{
            ?se <{ProvEntity.iri_specialization_of}> ?entity;
                <{ProvEntity.iri_generated_at_time}> ?time;
                <{ProvEntity.iri_description}> ?description.
            OPTIONAL {{
                ?se <{ProvEntity.iri_has_update_query}> ?updateQuery.
            }}
            VALUES ?entity {{ {values} }}
        }}
    """
    results = Sparql(query, config).run_select_query()
    output: dict[str, list[dict]] = {uri: [] for uri in entity_uris}
    for binding in results['results']['bindings']:
        entity_uri = binding['entity']['value']
        entry = {
            'time': binding['time']['value'],
            'updateQuery': binding['updateQuery']['value'] if 'updateQuery' in binding else None,
            'description': binding['description']['value'] if 'description' in binding else None,
        }
        output[entity_uri].append(entry)
    return output


def _batch_check_existence(entity_uris: set[str], config: dict) -> dict[str, bool]:
    values = _sparql_values(entity_uris)
    query = f"""
        SELECT DISTINCT ?entity
        WHERE {{
            VALUES ?entity {{ {values} }}
            ?entity ?p ?o.
        }}
    """
    results = Sparql(query, config).run_select_query()
    existing = {binding['entity']['value'] for binding in results['results']['bindings']}
    return {uri: uri in existing for uri in entity_uris}


def _build_delta_result(
    entity_str: str,
    snapshots: list[dict],
    exists: bool,
    on_time: tuple[str | None, str | None] | None,
    changed_properties: set[str],
) -> dict:
    output: dict[str, dict] = {}
    sorted_results = sorted(snapshots, key=lambda x: _parse_datetime(x['time']))
    relevant_results = _filter_timestamps_by_interval(
        on_time,
        [{'time': {'value': s['time']}} for s in snapshots],
        time_index='time',
    )
    if not relevant_results:
        return output
    output[entity_str] = {"created": None, "modified": {}, "deleted": None}
    creation_date = convert_to_datetime(sorted_results[0]['time'], stringify=True)
    if not exists:
        deletion_time = convert_to_datetime(sorted_results[-1]['time'], stringify=True)
        output[entity_str]["deleted"] = deletion_time
    relevant_times = {r['time']['value'] for r in relevant_results}
    for snap in sorted_results:
        if snap['time'] not in relevant_times:
            continue
        time_val = convert_to_datetime(snap['time'], stringify=True)
        if time_val != creation_date:
            update_query = snap['updateQuery']
            description = snap['description']
            if update_query and changed_properties:
                for changed_property in changed_properties:
                    if changed_property in update_query:
                        output[entity_str]["modified"][time_val] = update_query
            elif update_query and not changed_properties:
                output[entity_str]["modified"][time_val] = update_query
            elif not update_query and not changed_properties:
                output[entity_str]["modified"][time_val] = description
        else:
            output[entity_str]["created"] = creation_date
    return output


class AgnosticQuery:
    blazegraph_full_text_search: bool
    fuseki_full_text_search: bool
    virtuoso_full_text_search: bool
    graphdb_connector_name: str

    def __init__(self, query: str, on_time: tuple[str | None, str | None] | None = (None, None), other_snapshots: bool = False, config_path: str = CONFIG_PATH, config_dict: dict | None = None):
        self.query = query
        self.other_snapshots = other_snapshots
        self.config_path = config_path
        self.other_snapshots_metadata: dict = {}
        if config_dict is not None:
            self.config = config_dict
        else:
            with open(config_path, encoding="utf8") as json_file:
                self.config = json.load(json_file)
        self.__init_text_index(self.config)
        if on_time:
            after_time = convert_to_datetime(on_time[0], stringify=True)
            before_time = convert_to_datetime(on_time[1], stringify=True)
            self.on_time: tuple[str | None, str | None] | None = (after_time, before_time)  # type: ignore[assignment]
        else:
            self.on_time = None
        self.reconstructed_entities: set = set()
        self.vars_to_explicit_by_time: dict = {}
        self.relevant_entities_graphs: dict = {}
        self.relevant_graphs: dict[str, ConjunctiveGraph] = {}
        self._rebuild_relevant_graphs()

    def __init_text_index(self, config:dict):
        for full_text_search in ("blazegraph_full_text_search", "fuseki_full_text_search", "virtuoso_full_text_search"):
            ts_full_text_search:str = config[full_text_search]
            if ts_full_text_search.lower() in {"true", "1", 1, "t", "y", "yes", "ok"}:
                setattr(self, full_text_search, True)
            elif ts_full_text_search.lower() in {"false", "0", 0, "n", "f", "no"} or not ts_full_text_search:
                setattr(self, full_text_search, False)
            else:
                raise ValueError(f"Enter a valid value for '{full_text_search}' in the configuration file, for example 'yes' or 'no'.")
        self.graphdb_connector_name = config["graphdb_connector_name"]
        if len([index for index in [self.blazegraph_full_text_search, self.fuseki_full_text_search, self.virtuoso_full_text_search, self.graphdb_connector_name] if index]) > 1:
            raise ValueError("The use of multiple indexing systems simultaneously is currently not supported.")

    def _process_query(self) -> list[tuple]:
        algebra:CompValue = prepareQuery(self.query).algebra
        if algebra.name != "SelectQuery":
            raise ValueError("Only SELECT queries are allowed.")
        triples = []
        # The algebra can be extremely variable in case of one or more OPTIONAL in the query:
        # it is necessary to navigate the dictionary recursively in search of the values of the "triples" keys.
        self._tree_traverse(algebra, "triples", triples)
        triples_without_hook = [triple for triple in triples if isinstance(triple[0], Variable) and isinstance(triple[1], Variable) and isinstance(triple[2], Variable)]
        if triples_without_hook:
            raise ValueError("Could not perform a generic time agnostic query. Please, specify at least one URI or Literal within the query.")
        return triples

    def _tree_traverse(self, tree:dict, key:str, values:list[tuple]) -> None:
        for k, v in tree.items():
            if k == key:
                values.extend(v)
            elif isinstance(v, dict):
                self._tree_traverse(v, key, values)

    def _rebuild_relevant_graphs(self) -> None:
        triples_checked = set()
        all_isolated = True
        self.triples = self._process_query()
        for triple in self.triples:
            if self._is_isolated(triple) and self._is_a_new_triple(triple, triples_checked):
                present_entities = self._get_present_entities(triple)
                self._rebuild_relevant_entity(triple[0])
                self._find_entities_in_update_queries(triple, present_entities)
            else:
                all_isolated = False
                self._rebuild_relevant_entity(triple[0])
            triples_checked.add(triple)
        self._align_snapshots()
        if not all_isolated:
            self._solve_variables()

    def _is_isolated(self, triple:tuple) -> bool:
        if isinstance(triple[0], URIRef):
            return False
        variables = [el for el in triple if isinstance(el, Variable)]
        for variable in variables:
            other_triples = {t for t in self.triples if t != triple}
            if self._there_is_transitive_closure(variable, other_triples):
                return False
        return True

    def _there_is_transitive_closure(self, variable:Variable, triples:set[tuple]) -> bool:
        there_is_transitive_closure = False
        for triple in triples:
            if variable in triple and triple.index(variable) == 2:
                if isinstance(triple[0], URIRef):
                    return True
                elif isinstance(triple[0], Variable):
                    other_triples = {t for t in triples if t != triple}
                    there_is_transitive_closure = self._there_is_transitive_closure(triple[0], other_triples)
        return there_is_transitive_closure

    def _rebuild_relevant_entity(self, entity:URIRef | Literal) -> None:
        if isinstance(entity, URIRef) and entity not in self.reconstructed_entities:
            self.reconstructed_entities.add(entity)
            result = self._reconstruct_entity_state(entity)
            if result is not None:
                self._merge_entity_result(entity, *result)

    def _reconstruct_entity_state(self, entity: URIRef) -> tuple[dict, dict] | None:
        agnostic_entity = AgnosticEntity(entity, config=self.config, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
        if self.on_time:
            entity_graphs, _entity_snapshots, other_snapshots = agnostic_entity.get_state_at_time(time=self.on_time, include_prov_metadata=self.other_snapshots)
            return entity_graphs, other_snapshots
        entity_history = agnostic_entity.get_history(include_prov_metadata=True)
        return entity_history[0], {}

    def _merge_entity_result(self, entity: URIRef, entity_graphs: dict, other_snapshots: dict) -> None:
        if other_snapshots:
            self.other_snapshots_metadata.update(other_snapshots)
        if self.on_time:
            if entity_graphs:
                for relevant_timestamp, cg in entity_graphs.items():
                    self.relevant_entities_graphs.setdefault(entity, {})[relevant_timestamp] = cg
        else:
            if entity_graphs.get(entity):
                self.relevant_entities_graphs.update(entity_graphs)

    def _get_present_entities(self, triple: tuple) -> set:
        solvable_triple = [el.n3() for el in triple]
        variables = [el for el in triple if isinstance(el, Variable)]
        var_names_n3 = [el.n3() for el in variables]
        query = f"SELECT {' '.join(var_names_n3)} WHERE {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}}}"
        results = Sparql(query, self.config).run_select_query()
        bindings = results['results']['bindings']
        if isinstance(triple[1], InvPath):
            if isinstance(triple[2], Variable):
                var_name = str(triple[2])
                return {URIRef(b[var_name]['value']) for b in bindings if var_name in b and b[var_name]['type'] == 'uri'}
            return {triple[2]} if bindings else set()
        var_name = str(triple[0])
        return {URIRef(b[var_name]['value']) for b in bindings if var_name in b and b[var_name]['type'] == 'uri'}

    def _is_a_new_triple(self, triple:tuple, triples_checked:set) -> bool:
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        for triple_checked in triples_checked:
            uris_in_triple_checked = {el for el in triple_checked if isinstance(el, URIRef)}
            if not uris_in_triple.difference(uris_in_triple_checked):
                return False
        return True

    def _get_query_to_update_queries(self, triple:tuple) -> str:
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        query_to_identify = self.get_full_text_search(uris_in_triple)
        return query_to_identify

    def get_full_text_search(self, uris_in_triple:set) -> str:
        uris_in_triple = {str(el) for el in uris_in_triple}
        if self.blazegraph_full_text_search:
            query_to_identify = f'''
            PREFIX bds: <http://www.bigdata.com/rdf/search#>
            SELECT ?updateQuery
            WHERE {{
                ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
                ?updateQuery bds:search "{' '.join(uris_in_triple)}";
                    bds:matchAllTerms 'true'.
            }}
            '''
        elif self.fuseki_full_text_search:
            query_obj = '\\" AND \\"'.join(uris_in_triple)
            query_to_identify = f'''
                PREFIX text: <http://jena.apache.org/text#>
                SELECT ?updateQuery WHERE {{
                    ?se text:query "\\"{query_obj}\\"";
                        <{ProvEntity.iri_has_update_query}> ?updateQuery.
                }}
            '''
        elif self.virtuoso_full_text_search:
            query_obj = "' AND '".join(uris_in_triple)
            query_to_identify = f'''
            PREFIX bif: <bif:>
            SELECT ?updateQuery
            WHERE {{
                ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
                ?updateQuery bif:contains "'{query_obj}'".
            }}
            '''
        elif self.graphdb_connector_name:
            all = '\"'
            con_queries = f"con:query '{all}" + f"{all}'; con:query '{all}".join(uris_in_triple) + f"{all}'"
            query_to_identify = f'''
            PREFIX con: <http://www.ontotext.com/connectors/lucene#>
            PREFIX con-inst: <http://www.ontotext.com/connectors/lucene/instance#>
            SELECT ?updateQuery
            WHERE {{
                ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
                [] a con-inst:{self.graphdb_connector_name};
                    {con_queries};
                    con:entities ?snapshot.
            }}
            '''
        else:
            query_to_identify = f'''
            SELECT ?updateQuery
            WHERE {{
                ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
                {').'.join([f"FILTER CONTAINS (?updateQuery, '{uri}'" for uri in uris_in_triple])}).
            }}
            '''
        return query_to_identify

    def _find_entity_uris_in_update_queries(self, triple: tuple, entities: set) -> None:
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        uris_str = {str(el) for el in uris_in_triple}
        if not any([self.blazegraph_full_text_search, self.fuseki_full_text_search,
                     self.virtuoso_full_text_search, self.graphdb_connector_name]):
            filter_clauses = ".".join(
                f"FILTER CONTAINS (?uq, '{uri}')" for uri in uris_str
            )
            query = f"""
                SELECT ?entity WHERE {{
                    ?snapshot <{ProvEntity.iri_specialization_of}> ?entity;
                        <{ProvEntity.iri_has_update_query}> ?uq.
                    {filter_clauses}.
                }}
            """
            results = Sparql(query, self.config).run_select_query()
            for binding in results['results']['bindings']:
                entities.add(URIRef(binding['entity']['value']))
            return
        query_to_identify = self._get_query_to_update_queries(triple)
        results = Sparql(query_to_identify, self.config).run_select_query()
        for binding in results['results']['bindings']:
            uq = binding.get('updateQuery')
            if uq and uq.get('value'):
                for _, quads in _fast_parse_update(uq['value']):
                    for quad in quads:
                        triple_uris = {el for el in quad[:3] if isinstance(el, URIRef)}
                        if uris_in_triple.issubset(triple_uris):
                            entities.add(quad[0])

    def _find_entities_in_update_queries(self, triple:tuple, present_entities:set | None = None):
        if present_entities is None:
            present_entities = set()
        relevant_entities_found = present_entities
        self._find_entity_uris_in_update_queries(triple, relevant_entities_found)
        if relevant_entities_found:
            args_list = [
                (entity, self.config, self.on_time, self.other_snapshots)
                for entity in relevant_entities_found
                if isinstance(entity, URIRef)
            ]
            for result in _run_in_parallel(_reconstruct_entity_worker, args_list):
                if result is not None:
                    entity, entity_graphs, other_snapshots = result
                    self.reconstructed_entities.add(entity)
                    self._merge_entity_result(entity, entity_graphs, other_snapshots)

    def _solve_variables(self) -> None:
        self.vars_to_explicit_by_time = {}
        self._get_vars_to_explicit_by_time()
        while self._there_are_variables():
            solved_variables = self._explicit_solvable_variables()
            self._align_snapshots()
            if not solved_variables:
                return
            self._update_vars_to_explicit(solved_variables)
            self._get_vars_to_explicit_by_time()

    def _there_are_variables(self) -> bool:
        for triples in self.vars_to_explicit_by_time.values():
            for triple in triples:
                vars = [el for el in triple if isinstance(el, Variable)]
                if vars:
                    return True
        return False

    def _explicit_solvable_variables(self) -> dict:
        explicit_triples:dict[str, dict[str, set]] = {}
        for se, triples in self.vars_to_explicit_by_time.items():
            for triple in triples:
                variables = [el for el in triple if isinstance(el, Variable)]
                if len(variables) == 1:
                    solvable_triple = [el.n3() for el in triple]
                    variable = variables[0]
                    variable_index = triple.index(variable)
                    if variable_index == 2:
                        var_n3 = solvable_triple[2]
                        select_query = f"SELECT {var_n3} WHERE {{{solvable_triple[0]} {solvable_triple[1]} {var_n3}.}}"
                        select_results = list(self.relevant_graphs[se].query(select_query))
                        query_results = [(triple[0], triple[1], row[variable]) for row in select_results]  # type: ignore[call-overload]
                        for row in query_results:
                            explicit_triples.setdefault(se, {})
                            explicit_triples[se].setdefault(variable, set())
                            explicit_triples[se][variable].add(row)
                        args_list = [
                            (row[2], self.config, self.on_time, self.other_snapshots)
                            for row in query_results
                            if isinstance(row[2], URIRef) and row[2] not in self.reconstructed_entities
                        ]
                        for result_data in _run_in_parallel(_reconstruct_entity_worker, args_list):
                            if result_data is not None:
                                entity, entity_graphs, other_snapshots = result_data
                                self.reconstructed_entities.add(entity)
                                self._merge_entity_result(entity, entity_graphs, other_snapshots)
        return explicit_triples

    def _align_snapshots(self) -> None:
        # Merge entities based on snapshots
        for snapshots in self.relevant_entities_graphs.values():
            for snapshot, graph in snapshots.items():
                if snapshot in self.relevant_graphs:
                    for quad in graph.quads():
                        self.relevant_graphs[snapshot].add(quad)
                else:
                    self.relevant_graphs[snapshot] = graph
        # Propagate unchanged entities across timestamps: copy from tn to tn+1
        # when the entity hasn't changed (not deleted, just absent from that snapshot).
        # With a single timestamp there is nothing to propagate.
        if len(self.relevant_graphs) <= 1:
            return
        ordered_data = self._sort_relevant_graphs()
        for index, se_cg in enumerate(ordered_data):
            se = se_cg[0]
            cg = se_cg[1]
            if index > 0:
                previous_se = ordered_data[index-1][0]
                for subject in self.relevant_graphs[previous_se].subjects():
                    if (subject, None, None, None) not in cg and subject in self.relevant_entities_graphs and se not in self.relevant_entities_graphs[subject]:
                                for quad in self.relevant_graphs[previous_se].quads((subject, None, None, None)):
                                    self.relevant_graphs[se].add(quad)

    def _sort_relevant_graphs(self):
        ordered_data: list[tuple[str, ConjunctiveGraph]] = sorted(
            self.relevant_graphs.items(),
            key=lambda x: _parse_datetime(x[0]),
            reverse=False # That is, from past to present, assuming that the past influences the present and not the opposite like in Dark
        )
        return ordered_data

    def _update_vars_to_explicit(self, solved_variables:dict):
        vars_to_explicit_by_time: dict = {}
        for se, triples in self.vars_to_explicit_by_time.items():
            vars_to_explicit_by_time.setdefault(se, set())
            new_triples = set()
            for triple in triples:
                if se in solved_variables:
                    for solved_var, solved_triples in solved_variables[se].items():
                        if solved_var in triple:
                            for solved_triple in solved_triples:
                                new_triple = None
                                if solved_triple[0] != triple[0] and solved_triple[1] == triple[1]:
                                    continue
                                elif solved_triple[0] == triple[0] and solved_triple[1] == triple[1]:
                                    new_triple = solved_triple
                                else:
                                    new_triple = (solved_triple[2], triple[1], triple[2])
                                new_triples.add(new_triple)
                        elif not any(isinstance(el, Variable) for el in triple) or not any(var for var in solved_variables[se] if var in triple):
                            new_triples.add(triple)
            vars_to_explicit_by_time[se] = new_triples
        self.vars_to_explicit_by_time = vars_to_explicit_by_time

    def _get_vars_to_explicit_by_time(self) -> None:
        relevant_triples = None
        for se in self.relevant_graphs:
            if se not in self.vars_to_explicit_by_time:
                if relevant_triples is None:
                    relevant_triples = set()
                    for triple in self.triples:
                        if any(el for el in triple if isinstance(el, Variable) and not self._is_a_dead_end(el, triple)) and not self._is_isolated(triple):
                            relevant_triples.add(triple)
                self.vars_to_explicit_by_time[se] = set(relevant_triples)

    def _is_a_dead_end(self, el:URIRef | Variable | Literal, triple:tuple) -> bool:
        return isinstance(el, Variable) and triple.index(el) == 2 and not any(triple for triple in self.triples if el in triple if triple.index(el) == 0)


class VersionQuery(AgnosticQuery):
    """
    This class allows time-travel queries, both on a single version and all versions of the dataset.

    :param query: The SPARQL query string.
    :type query: str
    :param on_time: If you want to query a specific version, specify the time interval here. The format is (START, END). If one of the two values is None, only the other is considered. Finally, the time can be specified using any existing standard.
    :type on_time: Tuple[Union[str, None]], optional
    :param config_path: The path to the configuration file.
    :type config_path: str, optional
    """
    def __init__(self, query:str, on_time: tuple[str | None, str | None] | None = None, other_snapshots:bool=False, config_path:str=CONFIG_PATH, config_dict: dict | None = None):
        self._streaming_results: dict[str, list[dict]] = {}
        super().__init__(query, on_time, other_snapshots, config_path, config_dict)

    def _rebuild_relevant_graphs(self) -> None:
        self.triples = self._process_query()
        if self.on_time is not None:
            if (len(self.triples) == 1
                    and self._is_isolated(self.triples[0])
                    and not isinstance(self.triples[0][1], InvPath)
                    and not self.other_snapshots):
                self._rebuild_vm_batch()
                return
            super()._rebuild_relevant_graphs()
            return
        if not all(self._is_isolated(t) for t in self.triples):
            super()._rebuild_relevant_graphs()
            self._streaming_results = {
                str(convert_to_datetime(ts, stringify=True)): self._extract_bindings(g)
                for ts, g in self.relevant_graphs.items()
            }
            return
        self._rebuild_streaming()

    def _discover_entities_parallel(self, triple: tuple) -> set[str]:
        with ThreadPoolExecutor(max_workers=2) as executor:
            fut_present = executor.submit(self._get_present_entities, triple)
            entities_set: set = set()
            fut_prov = executor.submit(self._find_entity_uris_in_update_queries, triple, entities_set)
            present_entities = fut_present.result()
            fut_prov.result()
            all_entities = {str(e) for e in present_entities if isinstance(e, URIRef)}
            all_entities.update(str(e) for e in entities_set if isinstance(e, URIRef))

        return all_entities

    def _rebuild_vm_batch(self) -> None:
        assert self.on_time is not None
        triple = self.triples[0]
        all_entity_strs = self._discover_entities_parallel(triple)
        if not all_entity_strs:
            return
        with ThreadPoolExecutor(max_workers=2) as executor:
            fut_prov = executor.submit(_batch_query_provenance_snapshots, all_entity_strs, self.config)
            fut_data = executor.submit(_batch_query_dataset_triples, all_entity_strs, self.config)
            prov_data = fut_prov.result()
            dataset_data = fut_data.result()

        for entity_str in all_entity_strs:
            entity_uri = URIRef(entity_str)
            entity_graphs = _reconstruct_at_time_from_data(
                prov_data[entity_str], dataset_data[entity_str],
                self.on_time,
            )
            self.reconstructed_entities.add(entity_uri)
            for ts, cg in entity_graphs.items():
                self.relevant_entities_graphs.setdefault(entity_uri, {})[ts] = cg
        self._align_snapshots()

    def _extract_bindings(self, graph) -> list[dict]:
        query_results = graph.query(self.query)
        assert query_results.vars is not None
        vars_list = [str(var) for var in query_results.vars]
        output = []
        for result in query_results.bindings:
            binding = {}
            for var in vars_list:
                val = result.get(Variable(var))
                if val is not None:
                    binding[var] = Sparql._format_result_value(val)
            output.append(binding)
        return output

    def _rebuild_streaming(self) -> None:
        triples_checked = set()
        all_entity_strs: set[str] = set()
        use_fast_path = (
            len(self.triples) == 1
            and self._is_isolated(self.triples[0])
            and not isinstance(self.triples[0][1], InvPath)
        )
        for triple in self.triples:
            if self._is_a_new_triple(triple, triples_checked):
                present_entities = self._get_present_entities(triple)
                prov_entities: set = set()
                self._find_entity_uris_in_update_queries(triple, prov_entities)
                all_entity_strs.update(str(e) for e in present_entities if isinstance(e, URIRef))
                all_entity_strs.update(str(e) for e in prov_entities if isinstance(e, URIRef))
            triples_checked.add(triple)
        if not all_entity_strs:
            self._streaming_results = {}
            return
        if use_fast_path:
            with ThreadPoolExecutor(max_workers=2) as executor:
                fut_prov = executor.submit(_batch_query_provenance_snapshots, all_entity_strs, self.config)
                fut_data = executor.submit(_batch_query_dataset_triples, all_entity_strs, self.config)
                prov_data = fut_prov.result()
                dataset_data = fut_data.result()
            triple = self.triples[0]
            entity_bindings: dict[str, dict[str, list[dict]]] = {}
            for entity_str in all_entity_strs:
                per_ts: dict[str, list[dict]] = {}
                for ts, quad_set in _iter_versions_as_sets(prov_data[entity_str], dataset_data[entity_str]):
                    per_ts[ts] = _match_single_pattern(triple, quad_set)
                entity_bindings[entity_str] = per_ts
        else:
            entity_bindings = {}
            for entity_str in all_entity_strs:
                entity_uri = URIRef(entity_str)
                ae = AgnosticEntity(entity_uri, config=self.config, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
                per_ts = {}
                for ts, graph in ae.iter_versions():
                    per_ts[ts] = self._extract_bindings(graph)
                entity_bindings[entity_str] = per_ts
        all_timestamps: set[str] = set()
        for per_ts in entity_bindings.values():
            all_timestamps.update(per_ts.keys())
        sorted_timestamps = sorted(all_timestamps, key=_parse_datetime)
        result: dict[str, list[dict]] = {}
        last_known: dict[str, list[dict]] = {}
        for ts in sorted_timestamps:
            merged: list[dict] = []
            for entity_str, per_ts in entity_bindings.items():
                if ts in per_ts:
                    last_known[entity_str] = per_ts[ts]
                if entity_str in last_known:
                    merged.extend(last_known[entity_str])
            result[ts] = merged
        self._streaming_results = result

    def run_agnostic_query(self, include_all_timestamps: bool = False) -> tuple[dict[str, list[dict]], set]:
        if self.on_time is None:
            agnostic_result = self._streaming_results
            if include_all_timestamps:
                agnostic_result = self._fill_timestamp_gaps(agnostic_result)
            return agnostic_result, set()
        agnostic_result: dict[str, list[dict]] = {}
        for timestamp, graph in self.relevant_graphs.items():
            normalized = str(convert_to_datetime(timestamp, stringify=True))
            agnostic_result[normalized] = self._extract_bindings(graph)
        return agnostic_result, {data["generatedAtTime"] for _, data in self.other_snapshots_metadata.items()}

    def _get_all_provenance_timestamps(self) -> set:
        query = f"""
            SELECT ?time WHERE {{
                ?snapshot <{ProvEntity.iri_generated_at_time}> ?time .
            }}
        """
        results = Sparql(query, self.config).run_select_query()
        return {r['time']['value'] for r in results['results']['bindings']}

    def _fill_timestamp_gaps(self, result: dict) -> dict:
        all_timestamps = self._get_all_provenance_timestamps()
        sorted_result_ts = sorted(result.keys(), key=_parse_datetime)
        if not sorted_result_ts:
            return result
        min_ts = _parse_datetime(sorted_result_ts[0])
        relevant_timestamps = sorted(
            [t for t in all_timestamps if min_ts <= _parse_datetime(t)],
            key=_parse_datetime
        )
        filled = dict(result)
        last_known = None
        for ts in relevant_timestamps:
            normalized = convert_to_datetime(ts, stringify=True)
            if normalized in filled:
                last_known = normalized
            elif last_known is not None:
                filled[normalized] = filled[last_known]
        return filled


class DeltaQuery(AgnosticQuery):
    """
    This class allows single time and cross-time delta structured queries.

    :param query: A SPARQL query string. It is useful to identify the entities whose change you want to investigate.
    :type query: str
    :param on_time: If you want to query specific snapshots, specify the time interval here. The format is (START, END). If one of the two values is None, only the other is considered. Finally, the time can be specified using any existing standard.
    :type on_time: Tuple[Union[str, None]], optional
    :param changed_properties: A set of properties. It narrows the field to those entities where the properties specified in the set have changed.
    :type changed_properties: Set[str], optional
    :param config_path: The path to the configuration file.
    :type config_path: str, optional
    """
    def __init__(self, query:str, on_time: tuple[str | None, str | None] | None = None, changed_properties:set[str] | None=None, config_path:str = CONFIG_PATH, config_dict: dict | None = None):
        if changed_properties is None:
            changed_properties = set()
        super().__init__(query=query, on_time=on_time, config_path=config_path, config_dict=config_dict)
        self.changed_properties = changed_properties

    def _rebuild_relevant_graphs(self) -> None:
        triples_checked = set()
        self.triples = self._process_query()
        for triple in self.triples:
            if self._is_isolated(triple) and self._is_a_new_triple(triple, triples_checked):
                present_entities = self._get_present_entities(triple)
                self.reconstructed_entities.update(present_entities)
                self._find_entities_in_update_queries(triple)
            else:
                self._rebuild_relevant_entity(triple[0])
            triples_checked.add(triple)
        self._align_snapshots()
        self._solve_variables()

    def run_agnostic_query(self) -> dict:
        """
        Queries the deltas relevant to the query and the properties set
        in the specified time interval. If no property was indicated,
        any changes are considered. If no time interval was selected,
        the whole dataset's history is considered.
        The output has the following format: ::

            {
                RES_URI_1: {
                    "created": TIMESTAMP_CREATION,
                    "modified": {
                        TIMESTAMP_1: UPDATE_QUERY_1,
                        TIMESTAMP_2: UPDATE_QUERY_2,
                        TIMESTAMP_N: UPDATE_QUERY_N
                    },
                    "deleted": TIMESTAMP_DELETION
                },
                RES_URI_2: {
                    "created": TIMESTAMP_CREATION,
                    "modified": {
                        TIMESTAMP_1: UPDATE_QUERY_1,
                        TIMESTAMP_2: UPDATE_QUERY_2,
                        TIMESTAMP_N: UPDATE_QUERY_N
                    },
                    "deleted": TIMESTAMP_DELETION
                },
                RES_URI_N: {
                    "created": TIMESTAMP_CREATION,
                    "modified": {
                        TIMESTAMP_1: UPDATE_QUERY_1,
                        TIMESTAMP_2: UPDATE_QUERY_2,
                        TIMESTAMP_N: UPDATE_QUERY_N
                    },
                    "deleted": TIMESTAMP_DELETION
                },
            }

        :returns Dict[str, Set[Tuple]] -- The output is a dictionary that reports the modified entities, when they were created, modified, and deleted. Changes are reported as SPARQL UPDATE queries. If the entity was not created or deleted within the indicated range, the "created" or "deleted" value is None. On the other hand, if the entity does not exist within the input interval, the "modified" value is an empty dictionary.
        """
        entity_uris = {str(e) for e in self.reconstructed_entities}
        if not entity_uris:
            return {}
        prov_data = _batch_query_dm_provenance(entity_uris, self.config)
        existence_data = _batch_check_existence(entity_uris, self.config)
        output = {}
        for entity_str in entity_uris:
            snapshots = prov_data[entity_str]
            if not snapshots:
                continue
            exists = existence_data[entity_str]
            result = _build_delta_result(
                entity_str, snapshots, exists,
                self.on_time, self.changed_properties,
            )
            output.update(result)
        return output

def get_insert_query(graph_iri: URIRef, data: Graph) -> tuple[str, int]:
    num_of_statements: int = len(data)
    if num_of_statements <= 0:
        return "", 0
    else:
        statements: str = data.serialize(format="nt11", encoding="utf-8") \
            .decode("utf-8") \
            .replace('\n\n', '')
        insert_string: str = f"INSERT DATA {{ GRAPH <{graph_iri}> {{ {statements} }} }}"
        return insert_string, num_of_statements
