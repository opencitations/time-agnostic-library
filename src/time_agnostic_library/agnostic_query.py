#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2021, Arcangelo Massari <arcangelomas@gmail.com>
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

from typing import Set, Tuple, Dict, List, Union

import re, json
from copy import deepcopy
from SPARQLWrapper.Wrapper import SPARQLWrapper, POST, JSON, RDFXML
from rdflib.plugins.sparql.processor import prepareQuery
from rdflib.plugins.sparql.parser import parseUpdate
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib import ConjunctiveGraph, Graph, URIRef, Literal, Variable
from rdflib.paths import InvPath
from oc_ocdm.support.query_utils import get_insert_query
from tqdm import tqdm

from time_agnostic_library.sparql import Sparql
from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.agnostic_entity import AgnosticEntity, _filter_timestamps_by_interval

CONFIG_PATH = "./config.json"


class AgnosticQuery(object):
    def __init__(self, query:str, on_time:Tuple[Union[str, None]]=(None, None), config_path:str=CONFIG_PATH):
        self.query = query
        self.config_path = config_path
        with open(config_path, encoding="utf8") as json_file:
            config = json.load(json_file)
        blazegraph_full_text_search:str = config["blazegraph_full_text_search"]
        self.cache_triplestore_url:str = config["cache_triplestore_url"]
        if blazegraph_full_text_search.lower() in {"true", "1", 1, "t", "y", "yes", "ok"}:
            self.blazegraph_full_text_search = True
        elif blazegraph_full_text_search.lower() in {"false", "0", 0, "n", "f", "no"}:
            self.blazegraph_full_text_search = False
        else:
            raise ValueError("Enter a valid value for 'blazegraph_full_text_search' in the configuration file, for example 'yes' or 'no'.")
        if self.cache_triplestore_url:
            self.sparql = SPARQLWrapper(self.cache_triplestore_url)
        if on_time:
            after_time = AgnosticEntity._convert_to_datetime(on_time[0], stringify=True)
            before_time = AgnosticEntity._convert_to_datetime(on_time[1], stringify=True)
            self.on_time = (after_time, before_time)
        else:
            self.on_time = None
        self.reconstructed_entities = set()
        self.vars_to_explicit_by_time:Dict[str, Set[Tuple]] = dict()
        self.relevant_entities_graphs:Dict[URIRef, Dict[str, ConjunctiveGraph]] = dict()
        self.relevant_graphs:Dict[str, Union[ConjunctiveGraph, Set]] = dict()
        self.triples = self._process_query()
        self._rebuild_relevant_graphs()

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
            if self.cache_triplestore_url:
                relevant_timestamps_in_cache = self._get_relevant_timestamps_from_cache(entity)
                if relevant_timestamps_in_cache:
                    self._store_relevant_timestamps(entity, relevant_timestamps_in_cache)
                    return
            agnostic_entity = AgnosticEntity(entity, related_entities_history=False, config_path=self.config_path)
            if self.on_time:
                entity_at_time = agnostic_entity.get_state_at_time(time=self.on_time, include_prov_metadata=True)
                if entity_at_time[0]:
                    for relevant_timestamp, cg in entity_at_time[0].items():
                        if self.cache_triplestore_url:
                            # cache
                            self._cache_entity_graph(entity, cg, relevant_timestamp, entity_at_time[1])
                            self.relevant_graphs.setdefault(relevant_timestamp, set()).add(entity)
                        else:
                            # store in RAM
                            self.relevant_entities_graphs.setdefault(entity, dict())[relevant_timestamp] = cg
            else:
                entity_history = agnostic_entity.get_history(include_prov_metadata=True)
                if entity_history[0][entity]:
                    if self.cache_triplestore_url:
                        # cache
                        for entity, reconstructed_graphs in entity_history[0].items():
                            for timestamp, reconstructed_graph in reconstructed_graphs.items(): 
                                self._cache_entity_graph(entity, reconstructed_graph, timestamp, entity_history[1])
                                self.relevant_graphs.setdefault(timestamp, set()).add(entity)
                    else:
                        # store in RAM
                        self.relevant_entities_graphs.update(entity_history[0]) 

    def _get_query_to_identify(self, triple:list) -> str:
        solvable_triple = [el.n3() for el in triple]
        print(f"[VersionQuery:INFO] Rebuilding current relevant entities for the triple {solvable_triple}.")
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
            
    def _rebuild_relevant_graphs(self) -> None:
        # First, the graphs of the hooks are reconstructed
        triples_checked = set()
        for triple in self.triples:
            if self._is_isolated(triple) and self._is_a_new_triple(triple, triples_checked):
                query_to_identify = self._get_query_to_identify(triple)
                present_results = Sparql(query_to_identify, self.config_path).run_construct_query()
                pbar = tqdm(total=len(present_results))
                for result in present_results:
                    self._rebuild_relevant_entity(result[0])
                    self._rebuild_relevant_entity(result[2])
                    pbar.update(1)
                pbar.close()
                self._find_entities_in_update_queries(triple)
            else:
                self._rebuild_relevant_entity(triple[0])
                self._rebuild_relevant_entity(triple[2])
            triples_checked.add(triple)
        self._align_snapshots()
        # Then, the graphs of the entities obtained from the hooks are reconstructed
        self._solve_variables()
               
    def _get_query_to_update_queries(self, triple:tuple) -> str:
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        query_to_identify = f"""
            PREFIX bds: <http://www.bigdata.com/rdf/search#>
            SELECT DISTINCT ?updateQuery 
            WHERE {{
                ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
        """ 
        if self.blazegraph_full_text_search:
            bds_search = "?updateQuery bds:search '" + ' '.join(uris_in_triple) +  "'.?updateQuery bds:matchAllTerms 'true'.}"
            query_to_identify += bds_search
        else:
            filter_search = ").".join([f"FILTER CONTAINS (?updateQuery, '{uri}'" for uri in uris_in_triple]) + ").}"
            query_to_identify += filter_search
        for uri in uris_in_triple:
            if triple.index(uri) == 0 or triple.index(uri) == 2:
                self._rebuild_relevant_entity(uri)
        return query_to_identify

    def _find_entities_in_update_queries(self, triple:tuple):
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        relevant_entities_found = set()
        query_to_identify = self._get_query_to_update_queries(triple)
        results = Sparql(query_to_identify, self.config_path).run_select_query()
        if results:
            pbar = tqdm(total=len(results))
            print(f"[VersionQuery:INFO] Searching for relevant entities in relevant update queries.")
            for result in results:
                update = parseUpdate(result[0])
                for request in update["request"]:
                    for quadsNotTriples in request["quads"]["quadsNotTriples"]:
                        for triple in quadsNotTriples["triples"]:
                            triple = [el["string"] if "string" in el else el for el in triple]
                            relevant_entities = set(triple).difference(uris_in_triple) if len(uris_in_triple.intersection(triple)) == len(uris_in_triple) else None
                            if relevant_entities is not None:
                                relevant_entities_found.update(relevant_entities)
                pbar.update(1)
            pbar.close()
        new_entities_found = relevant_entities_found.difference(self.reconstructed_entities)
        if new_entities_found:
            print(f"[VersionQuery:INFO] Rebuilding relevant entities' history.")
            pbar = tqdm(total=len(new_entities_found))
            for new_entity_found in new_entities_found:
                self._rebuild_relevant_entity(new_entity_found)
                pbar.update(1)
            pbar.close()

    def _solve_variables(self) -> None:
        self._get_vars_to_explicit_by_time()
        while self._there_are_variables():
            solved_variables = self._explicit_solvable_variables()
            if not solved_variables:
                return 
            self._update_vars_to_explicit(solved_variables)

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
                        if self.cache_triplestore_url:
                            query_to_identify = f"""
                                CONSTRUCT {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}}}
                                WHERE {{GRAPH <https://github.com/opencitations/time-agnostic-library/{se}> {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}.}}}}
                            """
                            self.sparql.setQuery(query_to_identify)
                            self.sparql.setReturnFormat(RDFXML)
                            results = self.sparql.queryAndConvert()
                        else:
                            query_to_identify = f"""
                                CONSTRUCT {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}}}
                                WHERE {{{solvable_triple[0]} {solvable_triple[1]} {solvable_triple[2]}.}}
                            """
                            results = self.relevant_graphs[se].query(query_to_identify)
                        for result in results:
                            explicit_triples.setdefault(se, dict())
                            explicit_triples[se].setdefault(variable, set())
                            explicit_triples[se][variable].add(result)
                            self._rebuild_relevant_entity(result[variable_index])
                self._align_snapshots()
        return explicit_triples

    def _align_snapshots(self) -> None:
        if self.cache_triplestore_url:
            ordered_data = self._sort_relevant_graphs()
            # If an entity hasn't changed, copy it
            for index, se_entities in enumerate(ordered_data):
                se = se_entities[0]
                entities = se_entities[1]
                if index > 0:
                    previous_se = ordered_data[index-1][0]
                    for subject in self.relevant_graphs[previous_se]:
                        # To copy the entity two conditions must be met: 
                        #   1) the entity is present in tn but not in tn+1; 
                        #   2) the entity is absent in tn+1 because it has not changed 
                        #      and not because it has been deleted.
                        if subject not in entities:
                            query_subj_graph = f"""
                                CONSTRUCT {{<{subject}> ?p ?o}}
                                WHERE {{
                                    GRAPH <https://github.com/opencitations/time-agnostic-library/{previous_se}> {{<{subject}> ?p ?o}}
                                }}
                            """
                            self.sparql.setQuery(query_subj_graph)
                            self.sparql.setReturnFormat(RDFXML)
                            subj_graph = self.sparql.queryAndConvert()
                            self.relevant_graphs[se].add(subject)
                            self._cache_entity_graph(subject, subj_graph, se, dict())
        else:
            # Merge entities based on snapshots
            for _, snapshots in self.relevant_entities_graphs.items():
                for snapshot, graph in snapshots.items():
                    if snapshot in self.relevant_graphs:
                        for quad in graph.quads():
                            self.relevant_graphs[snapshot].add(quad)
                    else:
                        self.relevant_graphs[snapshot] = deepcopy(graph)
            # If an entity hasn't changed, copy it
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
                                    for quad in self.relevant_graphs[previous_se].quads((subject, None, None, None)):
                                        self.relevant_graphs[se].add(quad)

    def _sort_relevant_graphs(self):
        ordered_data: List[Tuple[str, ConjunctiveGraph]] = sorted(
            self.relevant_graphs.items(),
            key=lambda x: AgnosticEntity._convert_to_datetime(x[0]),
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
            self.vars_to_explicit_by_time[se] = set()
            for triple in self.triples:
                if any(el for el in triple if isinstance(el, Variable)) and not self._is_isolated(triple):
                    self.vars_to_explicit_by_time[se].add(triple)

    def _cache_entity_graph(self, entity:str, reconstructed_graph:ConjunctiveGraph, timestamp:str, prov_metadata:dict) -> None:
        graph_iri = f"https://github.com/opencitations/time-agnostic-library/{timestamp}"
        insert_query = get_insert_query(graph_iri=graph_iri, data=reconstructed_graph)[0]
        if insert_query:
            self.sparql.setQuery(insert_query)
            self.sparql.setMethod(POST)
            self.sparql.query()
        prov_graph = Graph()
        for en, metadata in prov_metadata.items(): 
            if str(ProvEntity.iri_generated_at_time) in metadata:
                self._cache_provenance(prov_graph, en, entity)
            else:
                for se, _ in metadata.items():
                    self._cache_provenance(prov_graph, se, en)
    
    def _cache_provenance(self, prov_graph:Graph, se_uri:str, entity:str) -> None:
        prov_graph.add((URIRef(se_uri), ProvEntity.iri_specialization_of, URIRef(entity)))
        insert_query = get_insert_query(graph_iri="", data=prov_graph)[0]
        self.sparql.setQuery(insert_query)
        self.sparql.setMethod(POST)
        self.sparql.query()

    def _get_relevant_timestamps_from_cache(self, entity:URIRef) -> set:
        relevant_timestamps = set()
        query_timestamps = f"""
            SELECT DISTINCT ?se
            WHERE {{
                ?se <{ProvEntity.iri_specialization_of}> <{entity}>.
            }}
        """
        query_provenance = f"""
            SELECT DISTINCT ?se ?timestamp
            WHERE {{
                ?se <{ProvEntity.iri_specialization_of}> <{entity}>;
                    <{ProvEntity.iri_generated_at_time}> ?timestamp.
            }}
        """
        self.sparql.setQuery(query_timestamps)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.queryAndConvert()
        provenance = Sparql(query = query_provenance, config_path=self.config_path).run_select_query()
        if len(results["results"]["bindings"]) == len(provenance) and len(provenance) > 0:
            for timestamp in provenance:
                relevant_timestamps.add(timestamp[1])
        return relevant_timestamps
    
    def _store_relevant_timestamps(self, entity:URIRef, relevant_timestamps:list):
        for timestamp in relevant_timestamps:
            timestamp = AgnosticEntity._convert_to_datetime(timestamp, stringify=True)
            self.relevant_graphs.setdefault(timestamp, set()).add(entity)
            self.reconstructed_entities.add(entity)

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
    def __init__(self, query:str, on_time:Tuple[Union[str, None]]="", config_path:str=CONFIG_PATH):
        super(VersionQuery, self).__init__(query, on_time, config_path)    
                
    def run_agnostic_query(self) -> Dict[str, Set[Tuple]]:
        """
        Run the query provided as a time-travel query. 
        If the **on_time** argument was specified, it runs on versions within the specified interval, on all versions otherwise.
        
        :returns Dict[str, Set[Tuple]] -- The output is a dictionary in which the keys are the snapshots relevant to that query. The values correspond to sets of tuples containing the query results at the time specified by the key. The positional value of the elements in the tuples is equivalent to the variables indicated in the query.
        """
        agnostic_result:dict[str, Set[Tuple]] = dict()
        if self.cache_triplestore_url:
            for timestamp, _ in self.relevant_graphs.items():
                output = set()
                split_by_where = re.split(pattern="where", string=self.query, maxsplit=1, flags=re.IGNORECASE)
                query_named_graph = split_by_where[0] + f"FROM <https://github.com/opencitations/time-agnostic-library/{timestamp}> WHERE" + split_by_where[1]
                self.sparql.setQuery(query_named_graph)
                self.sparql.setReturnFormat(JSON)
                results = self.sparql.queryAndConvert()
                for result_dict in results["results"]["bindings"]:
                    Sparql._get_tuples_set(self.query, result_dict, output)
                agnostic_result[timestamp] = output
        else:
            for snapshot, graph in self.relevant_graphs.items():
                results = graph.query(self.query)._get_bindings()
                output = set()
                for result in results:
                    Sparql._get_tuples_set(self.query, result, output)
                agnostic_result[snapshot] = output
        if self.on_time:
            agnostic_result_on_time = dict()
            relevant_timestamps = _filter_timestamps_by_interval(self.on_time, agnostic_result)
            for relevant_timestamp in relevant_timestamps:
                normalized_timestamp = AgnosticEntity._convert_to_datetime(relevant_timestamp, stringify=True)
                agnostic_result_on_time[normalized_timestamp] = agnostic_result[relevant_timestamp]
            return agnostic_result_on_time
        return agnostic_result


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
    def __init__(self, query:str, on_time:Tuple[Union[str, None]]=(), changed_properties:Set[str]=set(), config_path:str = CONFIG_PATH):
        super(DeltaQuery, self).__init__(query, on_time, config_path)    
        self.changed_properties = changed_properties

    def _rebuild_relevant_graphs(self) -> None:
        # First, the graphs of the hooks are reconstructed
        triples_checked = set()
        for triple in self.triples:
            if self._is_isolated(triple) and self._is_a_new_triple(triple, triples_checked):
                query_to_identify = self._get_query_to_identify(triple)
                present_results = Sparql(query_to_identify, self.config_path).run_construct_query()
                for result in present_results:
                    if isinstance(result[0], URIRef):
                        self.reconstructed_entities.add(result[0])
                    if isinstance(result[2], URIRef):
                        self.reconstructed_entities.add(result[2])
                self._find_entities_in_update_queries(triple)
            else:
                self._rebuild_relevant_entity(triple[0])
                self._rebuild_relevant_entity(triple[2])
            triples_checked.add(triple)
        self._align_snapshots()
        # Then, the graphs of the entities obtained from the hooks are reconstructed
        self._solve_variables()

    def _find_entities_in_update_queries(self, triple:tuple):
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        relevant_entities_found = set()
        query_to_identify = self._get_query_to_update_queries(triple)
        results = Sparql(query_to_identify, self.config_path).run_select_query()
        if results:
            for result in results:
                update = parseUpdate(result[0])
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

    def _get_query_to_update_queries(self, triple:tuple) -> str:
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        query_to_identify = f"""
            PREFIX bds: <http://www.bigdata.com/rdf/search#>
            SELECT DISTINCT ?updateQuery 
            WHERE {{
                ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
        """ 
        if self.blazegraph_full_text_search:
            bds_search = "?updateQuery bds:search '" + ' '.join(uris_in_triple) +  "'.?updateQuery bds:matchAllTerms 'true'.}"
            query_to_identify += bds_search
        else:
            filter_search = ").".join([f"FILTER CONTAINS (?updateQuery, '{uri}'" for uri in uris_in_triple]) + ").}"
            query_to_identify += filter_search
        for uri in uris_in_triple:
            if triple.index(uri) == 0 or triple.index(uri) == 2:
                self.reconstructed_entities.add(uri)
        return query_to_identify
        
    def _identify_changed_entities(self):
        output = dict()
        for identified_entity in self.reconstructed_entities:
            identified_entity = str(identified_entity)
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
                SELECT *
                WHERE {{
                    <{identified_entity}> ?p ?o.
                }}
            """
            results = Sparql(query, self.config_path).run_select_query()
            if results:
                results = sorted(list(results), key=lambda x:AgnosticEntity._convert_to_datetime(x[0]))
                output[identified_entity] = {
                    "created": AgnosticEntity._convert_to_datetime(results[0][0], stringify=True),
                    "modified": dict(), 
                    "deleted": None
                }
                exists = Sparql(query_existence, self.config_path).run_select_query()
                for i, result_tuple in enumerate(results):
                    if i > 0:
                        time = AgnosticEntity._convert_to_datetime(result_tuple[0], stringify=True)
                        if result_tuple[1] and self.changed_properties:
                            for changed_property in self.changed_properties:
                                if changed_property in result_tuple[1]:
                                    output[identified_entity]["modified"][time] = result_tuple[1]
                        elif result_tuple[1] and not self.changed_properties:
                            output[identified_entity]["modified"][time] = result_tuple[1]
                        elif not result_tuple[1] and not self.changed_properties:
                            output[identified_entity]["modified"][time] = result_tuple[2]
                if not exists:
                    output[identified_entity]["deleted"] = AgnosticEntity._convert_to_datetime(results[-1][0], stringify=True)            
        return output

    def run_agnostic_query(self) -> Dict[str, Dict[str, str]]:
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
        agnostic_result:Dict[str, Dict[str, str]] = self._identify_changed_entities()
        if self.on_time:
            agnostic_result_on_time = dict()
            for entity, operations in agnostic_result.items():
                agnostic_result_on_time[entity] = {
                    "created": None,
                    "modified": dict(), 
                    "deleted": None
                }
                for operation, value in operations.items():
                    if operation == "modified" and value:
                        relevant_timestamps = _filter_timestamps_by_interval(self.on_time, value)
                        for relevant_timestamp in relevant_timestamps:
                            update_query = agnostic_result[entity]["modified"][relevant_timestamp]
                            agnostic_result_on_time[entity]["modified"][relevant_timestamp] = update_query
                    elif (operation == "created" or operation == "deleted") and value:
                        relevant_deletion_time = _filter_timestamps_by_interval(self.on_time, [value])
                        if relevant_deletion_time:
                            agnostic_result_on_time[entity][operation] = relevant_deletion_time.pop()
            return agnostic_result_on_time
        else:
            return agnostic_result

    





    

