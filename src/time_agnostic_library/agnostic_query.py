#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022, Arcangelo Massari <arcangelo.massari@unibo.it>
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


from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from rdflib.plugins.sparql.processor import prepareQuery
from rdflib.plugins.sparql.parser import parseUpdate
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib import ConjunctiveGraph, Graph, URIRef, Literal, Variable
from rdflib.paths import InvPath
from SPARQLWrapper.Wrapper import SPARQLWrapper, POST, GET, JSON
from time_agnostic_library.agnostic_entity import AgnosticEntity, _filter_timestamps_by_interval
from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.sparql import Sparql
from tqdm import tqdm
from typing import Set, Tuple, Dict, List, Union
import json
import multiprocessing
import re


CONFIG_PATH = "./config.json"


class AgnosticQuery(object):
    def __init__(self, query:str, on_time:Tuple[Union[str, None]]=(None, None), config_path:str=CONFIG_PATH):
        self.query = query
        self.config_path = config_path
        with open(config_path, encoding="utf8") as json_file:
            config = json.load(json_file)
        self.__init_cache(config)
        self.__init_text_index(config)
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
        self.cache_insert_queries = list()
        self.triples = self._process_query()
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

    def __init_cache(self, config:dict):
        cache_triplestore_url = config["cache_triplestore_url"]
        self.cache_endpoint = cache_triplestore_url["endpoint"]
        if self.cache_endpoint:
            self.sparql_select = SPARQLWrapper(self.cache_endpoint)
            self.sparql_select.setMethod(GET)
            self.sparql_update = SPARQLWrapper(cache_triplestore_url['update_endpoint'])
            self.sparql_update.setMethod(POST)
            self.already_cached_entities = set()

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
        for triple in self.triples:
            if self._is_isolated(triple) and self._is_a_new_triple(triple, triples_checked):
                query_to_identify = self._get_query_to_identify(triple)
                present_results = Sparql(query_to_identify, self.config_path).run_construct_query()
                pbar = tqdm(total=len(present_results))
                for result in present_results:
                    self._rebuild_relevant_entity(result[0], isolated=True)
                    self._rebuild_relevant_entity(result[2], isolated=True)
                    pbar.update()
                pbar.close()
                self._find_entities_in_update_queries(triple)
            else:
                all_isolated = False
                self._rebuild_relevant_entity(triple[0], isolated=False)
                self._rebuild_relevant_entity(triple[2], isolated=False)
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

    def _rebuild_relevant_entity(self, entity:Union[URIRef, Literal], isolated:bool=False) -> None:
        if isinstance(entity, URIRef) and entity not in self.reconstructed_entities:
            self.reconstructed_entities.add(entity)
            if self.cache_endpoint:
                relevant_timestamps_in_cache = self._get_relevant_timestamps_from_cache(entity)
                if relevant_timestamps_in_cache:
                    for timestamp, cached_graph in relevant_timestamps_in_cache.items():
                        timestamp = AgnosticEntity._convert_to_datetime(timestamp, stringify=True)
                        self.reconstructed_entities.add(entity)
                        self.relevant_entities_graphs.setdefault(entity, dict())[timestamp] = cached_graph
                    self.already_cached_entities.add(entity)
                    return
            agnostic_entity = AgnosticEntity(entity, related_entities_history=False, config_path=self.config_path)
            if self.on_time:
                entity_at_time = agnostic_entity.get_state_at_time(time=self.on_time, include_prov_metadata=True)
                if entity_at_time[0]:
                    for relevant_timestamp, cg in entity_at_time[0].items():
                        if self.cache_endpoint:
                            self._cache_entity_graph(entity, cg, relevant_timestamp, entity_at_time[1])
                        # store in RAM
                        self.relevant_entities_graphs.setdefault(entity, dict())[relevant_timestamp] = cg
            else:
                entity_history = agnostic_entity.get_history(include_prov_metadata=True)
                if entity_history[0][entity]:
                    if self.cache_endpoint:
                        for entity, reconstructed_graphs in entity_history[0].items():
                            for timestamp, reconstructed_graph in reconstructed_graphs.items(): 
                                self._cache_entity_graph(entity, reconstructed_graph, timestamp, entity_history[1])
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

    def _get_query_to_update_queries(self, triple:tuple) -> str:
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        query_to_identify = self.get_full_text_search(uris_in_triple)
        for uri in uris_in_triple:
            if triple.index(uri) == 0 or triple.index(uri) == 2:
                self._rebuild_relevant_entity(uri, isolated=True)
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

    def _find_entities_in_update_queries(self, triple:tuple):
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        relevant_entities_found = set()
        query_to_identify = self._get_query_to_update_queries(triple)
        print(f"[VersionQuery:INFO] Searching for relevant entities in relevant update queries.")
        results = Sparql(query_to_identify, self.config_path).run_select_query()
        if results:
            for result in results:
                if result[0]:
                    update = parseUpdate(result[0])
                    for request in update["request"]:
                        for quadsNotTriples in request["quads"]["quadsNotTriples"]:
                            for triple in quadsNotTriples["triples"]:
                                triple = [el["string"] if "string" in el else el for el in triple]
                                relevant_entities = set(triple).difference(uris_in_triple) if len(uris_in_triple.intersection(triple)) == len(uris_in_triple) else None
                                if relevant_entities is not None:
                                    relevant_entities_found.update(relevant_entities)
        new_entities_found = relevant_entities_found.difference(self.reconstructed_entities)
        if new_entities_found:
            print(f"[VersionQuery:INFO] Rebuilding relevant entities' history.")
            pbar = tqdm(total=len(new_entities_found))
            with ThreadPoolExecutor() as executor:
                results = [executor.submit(self._rebuild_relevant_entity, new_entity_found, True) for new_entity_found in new_entities_found]
                for _ in as_completed(results):
                    pbar.update()
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
                            self._rebuild_relevant_entity(result[variable_index], isolated=False)
        return explicit_triples

    def _upload_data_to_cache(self):
        if self.cache_insert_queries:
            self.sparql_update.setQuery('; '.join(self.cache_insert_queries))
            self.sparql_update.query()
            self.cache_insert_queries = list()
    
    def _align_snapshots(self) -> None:
        print("[INFO: VersionQuery] Aligning snapshots")
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
                                if self.cache_endpoint:
                                    self._cache_entity_graph(subject, graph_to_cache, se, dict())
        
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
            if se not in self.vars_to_explicit_by_time:
                self.vars_to_explicit_by_time[se] = set()
                for triple in self.triples:
                    if any(el for el in triple if isinstance(el, Variable)) and not self._is_isolated(triple):
                        self.vars_to_explicit_by_time[se].add(triple)

    def _cache_entity_graph(self, entity:str, reconstructed_graph:ConjunctiveGraph, timestamp:str, prov_metadata:dict) -> None:
        if entity in self.already_cached_entities:
            return
        reconstructed_graph = deepcopy(reconstructed_graph)
        graph_iri = f"https://github.com/opencitations/time-agnostic-library/{timestamp}"
        graph_iri_relevant = f"https://github.com/opencitations/time-agnostic-library/relevant/{timestamp}"
        if not self.on_time:
            reconstructed_graph.add((URIRef(f"{entity}/cache"), URIRef("https://github.com/opencitations/time-agnostic-library/isComplete"), Literal("true")))
        insert_query = get_insert_query(graph_iri=graph_iri, data=reconstructed_graph)[0]
        prov = Graph()
        for en, metadata in prov_metadata.items(): 
            if 'generatedAtTime' in metadata:
                prov.add((URIRef(en), ProvEntity.iri_specialization_of, URIRef(entity)))
            else:
                for se, _ in metadata.items():
                    prov.add((URIRef(se), ProvEntity.iri_specialization_of, URIRef(en)))
        if insert_query:
            prov_insert_query = get_insert_query(graph_iri=graph_iri_relevant, data=prov)[0]
            insert_query = insert_query + ";" + prov_insert_query if prov_insert_query else insert_query
            self.cache_insert_queries.append(insert_query)

    def _get_relevant_timestamps_from_cache(self, entity:URIRef) -> Dict[str, Graph]:
        cached_graphs = dict()
        query_timestamps = f"""
        SELECT DISTINCT ?p ?o ?datatype ?c ?complete ?relevant
        WHERE {{
            GRAPH ?relevant {{?se <{ProvEntity.iri_specialization_of}> <{entity}>.}}
            GRAPH ?c {{<{URIRef(entity)}> ?p ?o. BIND (datatype(?o) AS ?datatype)}}
            <{entity}/cache> <https://github.com/opencitations/time-agnostic-library/isComplete> ?complete.
        }}
        """
        self.sparql_select.setQuery(query_timestamps)
        self.sparql_select.setReturnFormat(JSON)
        results = self.sparql_select.queryAndConvert()
        for result in results["results"]["bindings"]:
            if result["complete"]["value"] == "true":
                relevant_timestamp = result["relevant"]["value"].split("https://github.com/opencitations/time-agnostic-library/relevant/")[-1]
                cached_graphs.setdefault(relevant_timestamp, ConjunctiveGraph())
                found_timestamp = result["c"]["value"].split("https://github.com/opencitations/time-agnostic-library/")[-1]
                if found_timestamp == relevant_timestamp:
                    obj = result["o"]["value"]
                    obj = Literal(obj) if 'datatype' in result else URIRef(obj)
                    cached_graphs[found_timestamp].add((URIRef(entity), URIRef(result["p"]["value"]), obj))
        return cached_graphs

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

    def _query_reconstructed_graph(self, timestamp:str, graph:ConjunctiveGraph, agnostic_result:dict):
        output = set()
        if self.cache_endpoint:
            split_by_where = re.split(pattern="where", string=self.query, maxsplit=1, flags=re.IGNORECASE)
            query_named_graph = split_by_where[0] + f"FROM <https://github.com/opencitations/time-agnostic-library/{timestamp}> WHERE" + split_by_where[1]
            self.sparql_select.setQuery(query_named_graph)
            self.sparql_select.setReturnFormat(JSON)
            results = self.sparql_select.queryAndConvert()["results"]["bindings"]
        else:
            results = graph.query(self.query)._get_bindings()
        for result in results:
            Sparql._get_tuples_set(self.query, result, output)
        normalized_timestamp = AgnosticEntity._convert_to_datetime(timestamp, stringify=True)
        agnostic_result[normalized_timestamp] = output
        
    def run_agnostic_query(self) -> Dict[str, Set[Tuple]]:
        """
        Run the query provided as a time-travel query. 
        If the **on_time** argument was specified, it runs on versions within the specified interval, on all versions otherwise.
        
        :returns Dict[str, Set[Tuple]] -- The output is a dictionary in which the keys are the snapshots relevant to that query. The values correspond to sets of tuples containing the query results at the time specified by the key. The positional value of the elements in the tuples is equivalent to the variables indicated in the query.
        """
        print("[VersionQuery: INFO] Running agnostic query.")
        self._upload_data_to_cache()
        agnostic_result:dict[str, Set[Tuple]] = dict()
        relevant_timestamps = _filter_timestamps_by_interval(self.on_time, self.relevant_graphs)
        with ThreadPoolExecutor() as executor:
            [executor.submit(self._query_reconstructed_graph, timestamp, graph, agnostic_result) for timestamp, graph in self.relevant_graphs.items() if timestamp in relevant_timestamps]
        agnostic_result = {timestamp:{tuple(Literal(el, datatype=None) if isinstance(el, Literal) else el for el in result_tuple) for result_tuple in results} for timestamp, results in agnostic_result.items()}
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
                pbar = tqdm(total=len(present_results))
                for result in present_results:
                    if isinstance(result[0], URIRef):
                        self.reconstructed_entities.add(result[0])
                    if isinstance(result[2], URIRef):
                        self.reconstructed_entities.add(result[2])
                    pbar.update()
                pbar.close()
                self._find_entities_in_update_queries(triple)
            else:
                self._rebuild_relevant_entity(triple[0], isolated=False)
                self._rebuild_relevant_entity(triple[2], isolated=False)
            triples_checked.add(triple)
        self._align_snapshots()
        # Then, the graphs of the entities obtained from the hooks are reconstructed
        self._solve_variables()

    def _find_entities_in_update_queries(self, triple:tuple):
        uris_in_triple = {el for el in triple if isinstance(el, URIRef)}
        relevant_entities_found = set()
        query_to_identify = self._get_query_to_update_queries(triple)
        print(f"[DeltaQuery:INFO] Searching for relevant entities in relevant update queries.")
        results = Sparql(query_to_identify, self.config_path).run_select_query()
        if results:
            for result in results:
                if result[0]:
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

    def _get_query_to_identify(self, triple:list) -> str:
        solvable_triple = [el.n3() for el in triple]
        print(f"[DeltaQuery:INFO] Searching for current relevant entities for the triple {solvable_triple}.")
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
        for uri in uris_in_triple:
            if triple.index(uri) == 0 or triple.index(uri) == 2:
                self.reconstructed_entities.add(uri)
        return query_to_identify
    
    def __identify_changed_entities(self, identified_entity:URIRef):
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
            SELECT *
            WHERE {{
                <{identified_entity}> ?p ?o.
            }}
        """
        results = Sparql(query, self.config_path).run_select_query()
        if results:
            identified_entity = str(identified_entity)
            output[identified_entity] = {
                "created": None,
                "modified": dict(), 
                "deleted": None
            }
            results = sorted(list(results), key=lambda x:AgnosticEntity._convert_to_datetime(x[0]))
            creation_date = AgnosticEntity._convert_to_datetime(results[0][0], stringify=True)
            exists = Sparql(query_existence, self.config_path).run_select_query()
            if not exists:
                output[identified_entity]["deleted"] = AgnosticEntity._convert_to_datetime(results[-1][0], stringify=True)
            relevant_results = list(_filter_timestamps_by_interval(self.on_time, results, 0))
            if relevant_results:
                for result_tuple in relevant_results:
                    time = AgnosticEntity._convert_to_datetime(result_tuple[0], stringify=True)
                    if time != creation_date:
                        if result_tuple[1] and self.changed_properties:
                            for changed_property in self.changed_properties:
                                if changed_property in result_tuple[1]:
                                    output[identified_entity]["modified"][time] = result_tuple[1]
                        elif result_tuple[1] and not self.changed_properties:
                            output[identified_entity]["modified"][time] = result_tuple[1]
                        elif not result_tuple[1] and not self.changed_properties:
                            output[identified_entity]["modified"][time] = result_tuple[2]
                    else:
                        output[identified_entity]["created"] = creation_date
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
        output = dict()
        print(f"[DeltaQuery:INFO] Identifying changed entities.")
        pbar = tqdm(total=len(self.reconstructed_entities))
        l = multiprocessing.Lock()
        with ThreadPoolExecutor(initializer=init_lock, initargs=(l,)) as executor:
            futures = executor.map(self.__identify_changed_entities, self.reconstructed_entities)
            for future in futures:
                output.update(future)
                pbar.update()
        pbar.close()        
        return output

def init_lock(l):
    global lock
    lock = l

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
