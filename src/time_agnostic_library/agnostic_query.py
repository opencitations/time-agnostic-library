#!/usr/bin/python
# -*- coding: utf-8 -*-
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
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from typing import Dict, List, Set, Tuple, Union

from rdflib import ConjunctiveGraph, Graph, Literal, URIRef, Variable
from rdflib.paths import InvPath
from rdflib.plugins.sparql.parser import parseUpdate
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib.plugins.sparql.processor import prepareQuery
from time_agnostic_library.agnostic_entity import (
    AgnosticEntity, _filter_timestamps_by_interval)
from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.sparql import Sparql
from time_agnostic_library.support import convert_to_datetime
from tqdm import tqdm

CONFIG_PATH = "./config.json"


class AgnosticQuery(object):
    def __init__(self, query: str, on_time: Tuple[Union[str, None]] = (None, None), other_snapshots: bool = False, config_path: str = CONFIG_PATH, config_dict: dict = None):
        self.query = query
        self.other_snapshots = other_snapshots
        self.config_path = config_path
        self.other_snapshots_metadata = dict()
        if config_dict is not None:
            self.config = config_dict
        else:
            with open(config_path, encoding="utf8") as json_file:
                self.config = json.load(json_file)
        self.__init_text_index(self.config)
        if on_time:
            after_time = convert_to_datetime(on_time[0], stringify=True)
            before_time = convert_to_datetime(on_time[1], stringify=True)
            self.on_time = (after_time, before_time)
        else:
            self.on_time = None
        self.reconstructed_entities = set()
        self.vars_to_explicit_by_time:Dict[str, Set[Tuple]] = dict()
        self.relevant_entities_graphs:Dict[URIRef, Dict[str, ConjunctiveGraph]] = dict()
        self.relevant_graphs:Dict[str, Union[ConjunctiveGraph, Set]] = dict()
        self.cache_insert_queries = list()
        self._rebuild_relevant_graphs()
    
    def __init_text_index(self, config:dict):
        for full_text_search in {"blazegraph_full_text_search", "fuseki_full_text_search", "virtuoso_full_text_search"}:
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

    def _process_query(self) -> List[Tuple]:
        algebra:CompValue = prepareQuery(self.query).algebra
        if algebra.name != "SelectQuery":
            raise ValueError("Only SELECT queries are allowed.")
        triples = list()
        # The algebra can be extremely variable in case of one or more OPTIONAL in the query: 
        # it is necessary to navigate the dictionary recursively in search of the values of the "triples" keys.
        self._tree_traverse(algebra, "triples", triples)
        triples_without_hook = [triple for triple in triples if isinstance(triple[0], Variable) and isinstance(triple[1], Variable) and isinstance(triple[2], Variable)]
        if triples_without_hook:
            raise ValueError("Could not perform a generic time agnostic query. Please, specify at least one URI or Literal within the query.")
        return triples

    def _tree_traverse(self, tree:dict, key:str, values:List[Tuple]) -> None:
        for k, v in tree.items():
            if k == key:
                values.extend(v)
            elif isinstance(v, dict):
                found = self._tree_traverse(v, key, values)
                if found is not None:  
                    values.extend(found)

    def _rebuild_relevant_graphs(self) -> None:
        # First, the graphs of the hooks are reconstructed
        triples_checked = set()
        all_isolated = True
        self.triples = self._process_query()
        for triple in self.triples:
            if self._is_isolated(triple) and self._is_a_new_triple(triple, triples_checked):
                query_to_identify = self._get_query_to_identify(triple)
                present_results = Sparql(query_to_identify, self.config).run_construct_query()
                present_entities = {result[0] for result in present_results}
                self._rebuild_relevant_entity(triple[0])
                self._find_entities_in_update_queries(triple, present_entities)
            else:
                all_isolated = False
                self._rebuild_relevant_entity(triple[0])
            triples_checked.add(triple)
        self._align_snapshots()
        # Then, the graphs of the entities obtained from the hooks are reconstructed
        if not all_isolated:
            self._solve_variables()

    def _is_isolated(self, triple:tuple) -> bool:
        if isinstance(triple[0], URIRef):
            return False
        variables = [el for el in triple if isinstance(el, Variable)]
        if not variables:
            return False
        for variable in variables:
            other_triples = {t for t in self.triples if t != triple}
            if self._there_is_transitive_closure(variable, other_triples):
                return False
        return True

    def _there_is_transitive_closure(self, variable:Variable, triples:Set[Tuple]) -> bool:
        there_is_transitive_closure = False
        for triple in triples:
            if variable in triple and triple.index(variable) == 2:
                if isinstance(triple[0], URIRef):
                    return True
                elif isinstance(triple[0], Variable):
                    other_triples = {t for t in triples if t != triple}
                    there_is_transitive_closure = self._there_is_transitive_closure(triple[0], other_triples)
        return there_is_transitive_closure

    def _rebuild_relevant_entity(self, entity:Union[URIRef, Literal]) -> None:
        if isinstance(entity, URIRef) and entity not in self.reconstructed_entities:
            self.reconstructed_entities.add(entity)
            agnostic_entity = AgnosticEntity(entity, config=self.config, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
            if self.on_time:
                entity_graphs, entity_snapshots, other_snapshots = agnostic_entity.get_state_at_time(time=self.on_time, include_prov_metadata=self.other_snapshots)
                if other_snapshots:
                    self.other_snapshots_metadata.update(other_snapshots)
                if entity_graphs:
                    for relevant_timestamp, cg in entity_graphs.items():
                        self.relevant_entities_graphs.setdefault(entity, dict())[relevant_timestamp] = cg
            else:
                entity_history = agnostic_entity.get_history(include_prov_metadata=True)
                if entity_history[0][entity]:
                    self.relevant_entities_graphs.update(entity_history[0]) 

    def _get_query_to_identify(self, triple:list) -> str:
        solvable_triple = [el.n3() for el in triple]
        if isinstance(triple[1], InvPath):
            predicate = solvable_triple[1].replace("^", "", 1)
            query_to_identify = f"""
                CONSTRUCT {{{solvable_triple[2]} {predicate} {solvable_triple[0]}}}
                WHERE {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}}}
            """
        elif isinstance(triple[1], URIRef) or isinstance(triple[1], Variable):
            query_to_identify = f"""
                CONSTRUCT {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}}}
                WHERE {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}}}
            """
        return query_to_identify

    def _is_a_new_triple(self, triple:tuple, triples_checked:set) -> bool:
        for triple_checked in triples_checked:
            uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
            uris_in_triple_checked = {el for el in triple_checked if isinstance(el, URIRef)}
            new_uris = uris_in_triple.difference(uris_in_triple_checked)
            if not new_uris:
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
            SELECT DISTINCT ?updateQuery 
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
                SELECT DISTINCT ?updateQuery WHERE {{
                    ?se text:query "\\"{query_obj}\\"";
                        <{ProvEntity.iri_has_update_query}> ?updateQuery.
                }}
            '''
        elif self.virtuoso_full_text_search:
            query_obj = "' AND '".join(uris_in_triple)
            query_to_identify = f'''
            PREFIX bif: <bif:>
            SELECT DISTINCT ?updateQuery 
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
            SELECT DISTINCT ?updateQuery 
            WHERE {{
                ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
                [] a con-inst:{self.graphdb_connector_name};
                    {con_queries};
                    con:entities ?snapshot.
            }}
            '''
        else:
            query_to_identify = f'''
            SELECT DISTINCT ?updateQuery 
            WHERE {{
                ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
                {').'.join([f"FILTER CONTAINS (?updateQuery, '{uri}'" for uri in uris_in_triple])}).
            }}
            '''
        return query_to_identify

    def _find_entities_in_update_queries(self, triple:tuple, present_entities:set):
        def _process_triple(processed_triple, uris_in_triple: set, relevant_entities_found: set):
            processed_triple = [el["string"] if "string" in el else el for el in processed_triple]
            relevant_entities = {processed_triple[0]} if len(uris_in_triple.intersection(processed_triple)) == len(uris_in_triple) else None
            if relevant_entities is not None:
                relevant_entities_found.update(relevant_entities)
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        relevant_entities_found = present_entities
        query_to_identify = self._get_query_to_update_queries(triple)
        results = Sparql(query_to_identify, self.config).run_select_query()
        bindings = results['results']['bindings']
        if bindings:
            for result in bindings:
                update_query = result.get('updateQuery')
                if update_query and update_query.get('value'):
                    try:
                        update = parseUpdate(update_query['value'])
                    except Exception as e:
                        print(e)
                        print(update_query['value'])
                        raise
                    for request in update["request"]:
                        if "quadsNotTriples" in request["quads"]:
                            for quadsNotTriples in request["quads"]["quadsNotTriples"]:
                                for inner_triple in quadsNotTriples["triples"]:
                                    _process_triple(inner_triple, uris_in_triple, relevant_entities_found)
                        elif "triples" in request["quads"]:
                            for inner_triple in request["quads"]["triples"]:
                                _process_triple(inner_triple, uris_in_triple, relevant_entities_found)
        if relevant_entities_found:
            print(f"[VersionQuery:INFO] Rebuilding relevant entities' history.")
            pbar = tqdm(total=len(relevant_entities_found))
            for new_entity_found in relevant_entities_found:
                self._rebuild_relevant_entity(new_entity_found)
                pbar.update()
            # with ThreadPoolExecutor() as executor:
            #     results = [executor.submit(self._rebuild_relevant_entity, new_entity_found) for new_entity_found in relevant_entities_found]
            #     for _ in as_completed(results):
            #         pbar.update()
            pbar.close()
    
    def _solve_variables(self) -> None:
        self.vars_to_explicit_by_time = dict()
        self._get_vars_to_explicit_by_time()
        while self._there_are_variables():
            solved_variables = self._explicit_solvable_variables()
            self._align_snapshots()
            if not solved_variables:
                return 
            self._update_vars_to_explicit(solved_variables)
            self._get_vars_to_explicit_by_time()

    def _there_are_variables(self) -> bool:
        for _, triples in self.vars_to_explicit_by_time.items():
            for triple in triples:
                vars = [el for el in triple if isinstance(el, Variable)]
                if vars:
                    return True
        return False

    def _explicit_solvable_variables(self) -> Dict[str, Dict[str, str]]:
        explicit_triples:Dict[str, Dict[str, set]] = dict()
        for se, triples in self.vars_to_explicit_by_time.items():
            for triple in triples:
                variables = [el for el in triple if isinstance(el, Variable)]
                if len(variables) == 1:
                    solvable_triple = [el.n3() for el in triple]
                    variable = variables[0]
                    variable_index = triple.index(variable)
                    if variable_index == 2:
                        query_to_identify = f"""
                            CONSTRUCT {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}}}
                            WHERE {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}.}}
                        """
                        results = self.relevant_graphs[se].query(query_to_identify)
                        for result in results:
                            explicit_triples.setdefault(se, dict())
                            explicit_triples[se].setdefault(variable, set())
                            explicit_triples[se][variable].add(result)
                        with ThreadPoolExecutor() as executor:
                            executor.map(self._rebuild_relevant_entity, [result[variable_index] for result in results]) 
        return explicit_triples

    def _align_snapshots(self) -> None:
        # Merge entities based on snapshots
        for _, snapshots in self.relevant_entities_graphs.items():
            for snapshot, graph in snapshots.items():
                if snapshot in self.relevant_graphs:
                    for quad in graph.quads():
                        self.relevant_graphs[snapshot].add(quad)
                else:
                    self.relevant_graphs[snapshot] = deepcopy(graph)
        # To copy the entity two conditions must be met: 
        #   1) the entity is present in tn but not in tn+1; 
        #   2) the entity is absent in tn+1 because it has not changed and not because it has been deleted.
        ordered_data = self._sort_relevant_graphs()
        for index, se_cg in enumerate(ordered_data):
            se = se_cg[0]
            cg = se_cg[1]
            if index > 0:
                previous_se = ordered_data[index-1][0]
                for subject in self.relevant_graphs[previous_se].subjects():
                    if (subject, None, None, None) not in cg:
                        if subject in self.relevant_entities_graphs:
                            if se not in self.relevant_entities_graphs[subject]:
                                graph_to_cache = Graph()
                                for quad in self.relevant_graphs[previous_se].quads((subject, None, None, None)):
                                    self.relevant_graphs[se].add(quad)
                                    graph_to_cache.add(quad[:3])
        
    def _sort_relevant_graphs(self):
        ordered_data: List[Tuple[str, ConjunctiveGraph]] = sorted(
            self.relevant_graphs.items(),
            key=lambda x: convert_to_datetime(x[0]),
            reverse=False # That is, from past to present, assuming that the past influences the present and not the opposite like in Dark
        )
        return ordered_data   

    def _update_vars_to_explicit(self, solved_variables:Dict[str, Dict[Variable, Set[Tuple]]]):
        vars_to_explicit_by_time:Dict[str, Dict[Variable, set]] = dict()
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
                        elif not any(isinstance(el, Variable) for el in triple):
                            new_triples.add(triple)
                        elif not any(var for var in solved_variables[se] if var in triple):
                            new_triples.add(triple)
            vars_to_explicit_by_time[se] = new_triples 
        self.vars_to_explicit_by_time = vars_to_explicit_by_time

    def _get_vars_to_explicit_by_time(self) -> None:
        for se, _ in self.relevant_graphs.items():
            if se not in self.vars_to_explicit_by_time:
                self.vars_to_explicit_by_time[se] = set()
                for triple in self.triples:
                    if any(el for el in triple if isinstance(el, Variable) and not self._is_a_dead_end(el, triple)) and not self._is_isolated(triple):
                        self.vars_to_explicit_by_time[se].add(triple)
    
    def _is_a_dead_end(self, el:Union[URIRef, Variable, Literal], triple:tuple) -> bool:
        if isinstance(el, Variable):
            if triple.index(el) == 2 and not any(triple for triple in self.triples if el in triple if triple.index(el) == 0):
                return True
        return False


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
    def __init__(self, query:str, on_time:Tuple[Union[str, None]]="", other_snapshots=False, config_path:str=CONFIG_PATH, config_dict=None):
        super(VersionQuery, self).__init__(query, on_time, other_snapshots, config_path, config_dict)    

    def _query_reconstructed_graph(self, timestamp:str, graph:ConjunctiveGraph) -> tuple:
        output = set()
        query_results = graph.query(self.query)
        vars_list = query_results.vars
        results = query_results.bindings
        for result in results:
            Sparql._get_tuples_set(result, output, vars_list)
        normalized_timestamp = convert_to_datetime(timestamp, stringify=True)
        return normalized_timestamp, output
        
    def run_agnostic_query(self) -> Tuple[Dict[str, Set[Tuple]], dict]:
        """
        Run the query provided as a time-travel query. 
        If the **on_time** argument was specified, it runs on versions within the specified interval, on all versions otherwise.
        
        :returns Dict[str, Set[Tuple]] -- The output is a dictionary in which the keys are the snapshots relevant to that query. The values correspond to sets of tuples containing the query results at the time specified by the key. The positional value of the elements in the tuples is equivalent to the variables indicated in the query.
        """
        agnostic_result:dict[str, Set[Tuple]] = dict()
        with ThreadPoolExecutor() as executor:
            for future in [executor.submit(self._query_reconstructed_graph, timestamp, graph) for timestamp, graph in self.relevant_graphs.items()]:
                normalized_timestamp, output = future.result()
                agnostic_result[normalized_timestamp] = output
        agnostic_result = {timestamp:{tuple(Literal(el, datatype=None) if isinstance(el, Literal) else el for el in result_tuple) for result_tuple in results} for timestamp, results in agnostic_result.items()}
        return agnostic_result, {data["generatedAtTime"] for _, data in self.other_snapshots_metadata.items()}


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
    def __init__(self, query:str, on_time:Tuple[Union[str, None]]=(), changed_properties:Set[str]=set(), config_path:str = CONFIG_PATH, config_dict=None):
        super(DeltaQuery, self).__init__(query=query, on_time=on_time, config_path=config_path, config_dict=config_dict)    
        self.changed_properties = changed_properties

    def _rebuild_relevant_graphs(self) -> None:
        # First, the graphs of the hooks are reconstructed
        triples_checked = set()
        self.triples = self._process_query()
        for triple in self.triples:
            if self._is_isolated(triple) and self._is_a_new_triple(triple, triples_checked):
                query_to_identify = self._get_query_to_identify(triple)
                present_results = Sparql(query_to_identify, self.config).run_construct_query()
                pbar = tqdm(total=len(present_results))
                for result in present_results:
                    if isinstance(result[0], URIRef):
                        self.reconstructed_entities.add(result[0])
                    pbar.update()
                pbar.close()
                if isinstance(triple[0], URIRef):
                    self.reconstructed_entities.add(triple[0])
                self._find_entities_in_update_queries(triple)
            else:
                self._rebuild_relevant_entity(triple[0])
            triples_checked.add(triple)
        self._align_snapshots()
        # Then, the graphs of the entities obtained from the hooks are reconstructed
        self._solve_variables()

    def _find_entities_in_update_queries(self, triple:tuple):
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        relevant_entities_found = set()
        query_to_identify = self._get_query_to_update_queries(triple)
        results = Sparql(query_to_identify, self.config).run_select_query()
        bindings = results['results']['bindings']
        if bindings:
            for result in bindings:
                update_query = result.get('updateQuery')
                if update_query and update_query.get('value'):
                    update = parseUpdate(update_query['value'])
                    for request in update["request"]:
                        for quadsNotTriples in request["quads"]["quadsNotTriples"]:
                            for triple in quadsNotTriples["triples"]:
                                triple = [el["string"] if "string" in el else el for el in triple]
                                relevant_entities = set(triple).difference(uris_in_triple) if len(uris_in_triple.intersection(triple)) == len(uris_in_triple) else None
                                if relevant_entities is not None:
                                    relevant_entities_found.update(relevant_entities)
        for relevant_entity_found in relevant_entities_found:
            if isinstance(relevant_entity_found, URIRef):
                self.reconstructed_entities.add(relevant_entity_found)

    def _get_query_to_identify(self, triple:list) -> str:
        solvable_triple = [el.n3() for el in triple]
        if isinstance(triple[1], InvPath):
            predicate = solvable_triple[1].replace("^", "", 1)
            query_to_identify = f"""
                CONSTRUCT {{{solvable_triple[2]} {predicate} {solvable_triple[0]}}}
                WHERE {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}}}
            """
        elif isinstance(triple[1], URIRef) or isinstance(triple[1], Variable):
            query_to_identify = f"""
                CONSTRUCT {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}}}
                WHERE {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}}}
            """
        return query_to_identify

    def _get_query_to_update_queries(self, triple:tuple) -> str:
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        query_to_identify = self.get_full_text_search(uris_in_triple)
        return query_to_identify
    
    def __identify_changed_entities(self, identified_entity: URIRef):
        output = dict()
        query = f"""
            SELECT DISTINCT ?time ?updateQuery ?description
            WHERE {{
                ?se <{ProvEntity.iri_specialization_of}> <{identified_entity}>;
                    <{ProvEntity.iri_generated_at_time}> ?time;
                    <{ProvEntity.iri_description}> ?description.
                OPTIONAL {{
                    ?se <{ProvEntity.iri_has_update_query}> ?updateQuery.
                }}
            }}
        """
        query_existence = f"""
            ASK WHERE {{
                <{identified_entity}> ?p ?o.
            }}
        """
        # Run the query and get the bindings
        results = Sparql(query, self.config).run_select_query()
        bindings = results['results']['bindings']
        if bindings:
            # Filter timestamps by interval
            relevant_results = _filter_timestamps_by_interval(self.on_time, bindings, time_index='time')
            if relevant_results:
                identified_entity_str = str(identified_entity)
                output[identified_entity_str] = {
                    "created": None,
                    "modified": dict(),
                    "deleted": None
                }
                # Sort the results by time
                results_sorted = sorted(bindings, key=lambda x: convert_to_datetime(x['time']['value']))
                creation_date = convert_to_datetime(results_sorted[0]['time']['value'], stringify=True)
                # Check if the entity exists
                exists = Sparql(query_existence, self.config).run_ask_query()
                if not exists:
                    # Entity has been deleted
                    deletion_time = convert_to_datetime(results_sorted[-1]['time']['value'], stringify=True)
                    output[identified_entity_str]["deleted"] = deletion_time
                for result_binding in relevant_results:
                    time = convert_to_datetime(result_binding['time']['value'], stringify=True)
                    if time != creation_date:
                        update_query = result_binding.get('updateQuery', {}).get('value')
                        description = result_binding.get('description', {}).get('value')
                        if update_query and self.changed_properties:
                            for changed_property in self.changed_properties:
                                if changed_property in update_query:
                                    output[identified_entity_str]["modified"][time] = update_query
                        elif update_query and not self.changed_properties:
                            output[identified_entity_str]["modified"][time] = update_query
                        elif not update_query and not self.changed_properties:
                            output[identified_entity_str]["modified"][time] = description
                    else:
                        output[identified_entity_str]["created"] = creation_date
        return output
        
    def run_agnostic_query(self) -> Tuple[Dict[str, Dict[str, str]], dict]:
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
        output = dict()
        print(f"[DeltaQuery:INFO] Identifying changed entities.")
        pbar = tqdm(total=len(self.reconstructed_entities))
        with ThreadPoolExecutor() as executor:
            futures = executor.map(self.__identify_changed_entities, self.reconstructed_entities)
            for future in futures:
                output.update(future)
                pbar.update()
        pbar.close()        
        return output

def get_insert_query(graph_iri: URIRef, data: Graph) -> Tuple[str, int]:
    num_of_statements: int = len(data)
    if num_of_statements <= 0:
        return "", 0
    else:
        statements: str = data.serialize(format="nt11", encoding="utf-8") \
            .decode("utf-8") \
            .replace('\n\n', '')
        insert_string: str = f"INSERT DATA {{ GRAPH <{graph_iri}> {{ {statements} }} }}"
        return insert_string, num_of_statements