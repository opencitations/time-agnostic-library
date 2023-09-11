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

import copy
import re
from typing import Dict, List, Set, Tuple, Union

from rdflib.graph import ConjunctiveGraph, Graph
from rdflib.plugins.sparql.processor import processUpdate
from rdflib.term import URIRef

from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.sparql import Sparql
from time_agnostic_library.support import convert_to_datetime

CONFIG_PATH = "./config.json"

class AgnosticEntity:
    """
    The entity of which you want to materialize one or all versions, 
    based on the provenance snapshots available for that entity.
    
    :param res: The URI of the entity
    :type res: str
    :param related_entities_history: True, if you also want to return information on related entities, those that have the URI of the res parameter as an object, False otherwise.
    :type related_entities_history: bool, optional
    :param config_path: The path to the configuration file.
    :type config_path: str, optional
    """

    def __init__(self, res:str, related_entities_history:bool=False, config_path:str=CONFIG_PATH):
        self.res = res
        self.related_entities_history = related_entities_history
        self.config_path = config_path

    def get_history(self, include_prov_metadata:bool=False) -> Tuple[Dict[str, Dict[str, Graph]], Dict[str, Dict[str, Dict[str, str]]]]:
        """
        It materializes all versions of an entity. If **related_entities_history** is True, 
        it also materializes all versions of all related entities, 
        which have **res** as object rather than subject. 
        If **include_prov_metadata** is True, 
        the provenance metadata of the returned entity/entities is also returned.
        The output has the following format: ::

            (
                {
                    RES_URI: {
                            TIME_1: ENTITY_GRAPH_AT_TIME_1, 
                            TIME_2: ENTITY_GRAPH_AT_TIME_2
                    }
                },
                {
                    RES_URI: {
                        SNAPSHOT_URI_AT_TIME_1': {
                            'generatedAtTime': GENERATION_TIME, 
                            'wasAttributedTo': ATTRIBUTION, 
                            'hadPrimarySource': PRIMARY_SOURCE
                        }, 
                        SNAPSHOT_URI_AT_TIME_2: {
                            'generatedAtTime': GENERATION_TIME, 
                            'wasAttributedTo': ATTRIBUTION, 
                            'hadPrimarySource': PRIMARY_SOURCE
                    }
                } 
            )

        :returns:  Tuple[dict, Union[dict, None]] -- The output is always a two-element tuple. The first is a dictionary containing all the versions of a given resource. The second is a dictionary containing all the provenance metadata linked to that resource if **include_prov_metadata** is True, None if False.
        """
        if self.related_entities_history:
            entities_to_query = {self.res}
            current_state = self._query_dataset()
            related_entities = current_state.triples(
                (None, None, URIRef(self.res)))
            for entity in related_entities:
                if ProvEntity.PROV not in entity[1]:
                    entities_to_query.add(str(entity[0]))
            return _get_entities_histories(entities_to_query, include_prov_metadata, self.config_path)
        entity_history = self._get_entity_current_state(include_prov_metadata)
        entity_history = self._get_old_graphs(entity_history)
        return tuple(entity_history)

    def get_state_at_time(
        self, 
        time: Tuple[Union[str, None]], 
        include_prov_metadata: bool = False, 
        ) -> Tuple[Graph, dict, Union[dict, None]]:
        """
        Given a time interval, the function returns the resource's states within the interval, the returned snapshots metadata
        and, optionally, the hooks to the previous and subsequent snapshots.
        The time can be specified in any existing standard. 
        Snapshot metadata includes generation time, the responsible agent and the primary source.
        The output has the following format: ::

            (
                {
                    TIME_1: ENTITY_GRAPH_AT_TIME_1, 
                    TIME_2: ENTITY_GRAPH_AT_TIME_2
                },
                {
                    SNAPSHOT_URI_AT_TIME_1: {
                        'generatedAtTime': TIME_1, 
                        'wasAttributedTo': ATTRIBUTION, 
                        'hadPrimarySource': PRIMARY_SOURCE
                    },
                    SNAPSHOT_URI_AT_TIME_2: {
                        'generatedAtTime': TIME_2, 
                        'wasAttributedTo': ATTRIBUTION, 
                        'hadPrimarySource': PRIMARY_SOURCE
                    }
                }, 
                {
                    OTHER_SNAPSHOT_URI_1: {
                        'generatedAtTime': GENERATION_TIME, 
                        'wasAttributedTo': ATTRIBUTION, 
                        'hadPrimarySource': PRIMARY_SOURCE
                    }, 
                    OTHER_SNAPSHOT_URI_2: {
                        'generatedAtTime': GENERATION_TIME, 
                        'wasAttributedTo': ATTRIBUTION, 
                        'hadPrimarySource': PRIMARY_SOURCE
                    }
                }
            )

        :param time: A time interval, in the form (START, END). If one of the two values is None, only the other is considered. The time can be specified using any existing standard.
        :type time: Tuple[Union[str, None]].
        :param include_prov_metadata: If True, hooks are returned to the previous and subsequent snapshots.
        :type include_prov_metadata: bool, optional
        :returns: Tuple[dict, dict, Union[dict, None]] -- The method always returns a tuple of three elements: the first is a dictionary that associates graphs and timestamps within the specified interval; the second contains the snapshots metadata of which the states has been returned. If the **include_prov_metadata** parameter is True, the third element of the tuple is the metadata on the other snapshots, otherwise an empty dictionary. The third dictionary is empty also if only one snapshot exists.
        """
        query_snapshots = f"""
            SELECT ?snapshot ?time ?responsibleAgent ?updateQuery ?primarySource ?description
            WHERE {{
                ?snapshot <{ProvEntity.iri_specialization_of}> <{self.res}>;
                    <{ProvEntity.iri_generated_at_time}> ?time;
                    <{ProvEntity.iri_was_attributed_to}> ?responsibleAgent.
                OPTIONAL {{
                    ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
                }}
                OPTIONAL {{
                    ?snapshot <{ProvEntity.iri_had_primary_source}> ?primarySource.
                }}
            }}
        """
        results = list(Sparql(query_snapshots, config_path=self.config_path).run_select_query())
        if not results:
            return None, None, None
        results.sort(key=lambda x:convert_to_datetime(x[1]), reverse=True)
        relevant_results = _filter_timestamps_by_interval(time, results, time_index=1)
        other_snapshots_metadata = None
        entity_snapshots = dict()
        entity_graphs = dict()
        if include_prov_metadata:
            other_snapshots = [snapshot for snapshot in results if snapshot[0] not in {relevant_result[0] for relevant_result in relevant_results}]
            other_snapshots_metadata = dict()
            for other_snapshot in other_snapshots:
                other_snapshots_metadata[other_snapshot[0]] = {
                    "generatedAtTime": other_snapshot[1],
                    "wasAttributedTo": other_snapshot[2],
                    "hadPrimarySource": other_snapshot[4]
            }
        if not relevant_results:
            return entity_graphs, entity_snapshots, other_snapshots_metadata
        entity_cg = self._query_dataset()
        for relevant_result in relevant_results:
            sum_update_queries = ""
            for result in results:
                if result[3]:
                    if convert_to_datetime(result[1]) > convert_to_datetime(relevant_result[1]):
                        sum_update_queries += (result[3]) +  ";"
            entity_present_graph = copy.deepcopy(entity_cg)
            if sum_update_queries:
                self._manage_update_queries(entity_present_graph, sum_update_queries)
            entity_graphs[convert_to_datetime(relevant_result[1], stringify=True)] = entity_present_graph
            entity_snapshots[relevant_result[0]] = {
                "generatedAtTime": relevant_result[1],
                "wasAttributedTo": relevant_result[2],
                "hadPrimarySource": relevant_result[4]
            }
        return entity_graphs, entity_snapshots, other_snapshots_metadata
    
    def _include_prov_metadata(self, triples_generated_at_time:list, current_state:ConjunctiveGraph) -> dict:
        if list(current_state.triples((URIRef(self.res), URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), ProvEntity.iri_entity))): 
            return dict()
        prov_properties = {
            ProvEntity.iri_was_attributed_to: "wasAttributedTo", 
            ProvEntity.iri_had_primary_source: "hadPrimarySource", 
            ProvEntity.iri_description: "description"
        }
        prov_metadata = {
            self.res: dict()
        }
        for triple in triples_generated_at_time:
            time = convert_to_datetime(triple[2]).strftime("%Y-%m-%dT%H:%M:%S")
            prov_metadata[self.res][str(triple[0])] = {
                "generatedAtTime": time,
                "wasAttributedTo": None,
                "hadPrimarySource": None,
                "description": None
            }
        for entity, metadata in dict(prov_metadata).items():
            for se, _ in metadata.items():
                for prov_property, abbr in prov_properties.items():
                    triples_with_property = current_state.triples(
                    (URIRef(se), prov_property, None))
                    for triple in triples_with_property:
                        prov_metadata[entity][str(triple[0])][abbr] = str(triple[2])
        return prov_metadata

    def _get_entity_current_state(self, include_prov_metadata:bool=False) -> list:
        entity_current_state = [{self.res: dict()}]
        current_state = ConjunctiveGraph()
        for quad in self._query_provenance(include_prov_metadata).quads():
            current_state.add(quad)
        if len(current_state) == 0:
            entity_current_state.append(dict())
            return entity_current_state
        for quad in self._query_dataset().quads():
            current_state.add(quad)
        triples_generated_at_time = list(current_state.triples(
            (None, ProvEntity.iri_generated_at_time, None)))
        most_recent_time = None
        for triple in triples_generated_at_time:
            snapshot_time = triple[2]
            snapshot_date_time = convert_to_datetime(snapshot_time)
            if most_recent_time:
                if snapshot_date_time > convert_to_datetime(most_recent_time):
                    most_recent_time = snapshot_time
            elif not most_recent_time:
                most_recent_time = snapshot_time
            entity_current_state[0][self.res][snapshot_time] = None
        entity_current_state[0][self.res][most_recent_time] = current_state
        if include_prov_metadata:
            prov_metadata = self._include_prov_metadata(triples_generated_at_time, current_state)
            entity_current_state.append(prov_metadata)
        else:
            entity_current_state.append(None)
        return entity_current_state

    def _get_old_graphs(self, entity_current_state:List[Dict[str, Dict[str, ConjunctiveGraph]]]) -> list:
        ordered_data: List[Tuple[str, ConjunctiveGraph]] = sorted(
            entity_current_state[0][self.res].items(),
            key=lambda x: convert_to_datetime(x[0]),
            reverse=True
        )
        for index, date_graph in enumerate(ordered_data):
            if index > 0:
                next_snapshot = ordered_data[index-1][0]
                previous_graph: ConjunctiveGraph = copy.deepcopy(entity_current_state[0][self.res][next_snapshot])
                snapshot_uri = list(previous_graph.subjects(object=next_snapshot))[0]
                snapshot_update_query: str = previous_graph.value(
                    subject=snapshot_uri,
                    predicate=ProvEntity.iri_has_update_query,
                    object=None)
                if snapshot_update_query is None:
                    entity_current_state[0][self.res][date_graph[0]] = previous_graph
                else:
                    self._manage_update_queries(previous_graph, snapshot_update_query)
                    entity_current_state[0][self.res][date_graph[0]] = previous_graph
        for time in list(entity_current_state[0][self.res]):
            cg_no_pro = entity_current_state[0][self.res].pop(time)
            for prov_property in ProvEntity.get_prov_properties():
                cg_no_pro.remove((None, prov_property, None))
            time_no_tz = convert_to_datetime(time)
            entity_current_state[0][self.res][time_no_tz.strftime("%Y-%m-%dT%H:%M:%S")] = cg_no_pro
        return entity_current_state

    @classmethod
    def _manage_update_queries(cls, graph: ConjunctiveGraph, update_query: str) -> None:
        update_query = update_query.replace("INSERT", "%temp%").replace("DELETE", "INSERT").replace("%temp%", "DELETE")
        triples_end_pattern = r">\s*\."
        operations_pattern = r"((?:DELETE|INSERT)\s(?:DATA)\s?{\s?(?:GRAPH)\s?<[\w\W]+?>\s{)"
        matches = re.split(triples_end_pattern, update_query)
        # 90 is the maximum number of triples after which recursion error occurs
        if len(matches) > 90:
            split_by_operations = re.split(operations_pattern, update_query, flags=re.IGNORECASE)
            # The operations are the odd elements of the list
            operations = split_by_operations[1::2]
            start = 0
            for operation in operations:
                operation_index = split_by_operations.index(operation, start)
                start = operation_index + 1
                triples = split_by_operations[operation_index + 1]
                operation_and_query = operation + triples
                matches_with_operation = re.split(triples_end_pattern, operation_and_query)
                if len(matches_with_operation) > 90:
                    # Remove operation and trailing "} }"
                    matches_no_operation = [match.replace(operation, "") for match in matches_with_operation][:-1]
                    while len(matches_no_operation) > 0:
                        cut_update_query = operation + "> .".join(matches_no_operation[:90]) + "> .} }"
                        try:
                            processUpdate(graph, cut_update_query)
                        except Exception:
                            print(update_query)
                        matches_no_operation = matches_no_operation[90:]
                else:
                    processUpdate(graph, operation_and_query)      
        else:
            processUpdate(graph, update_query)

    def _query_dataset(self) -> ConjunctiveGraph:
        # A SELECT hack can be used to return RDF quads in named graphs,
        # since the CONSTRUCT allows only to return triples in SPARQL 1.1.
        # Here is an exemple of SELECT hack:
        #
        # SELECT ?s ?p ?o ?c
        # WHERE {
        #     BIND (<{res}> AS ?s)
        #     GRAPH ?c {?s ?p ?o}
        # }
        #
        # Aftwerwards, the rdflib add method can be used to add quads to a Conjunctive Graph,
        # where the fourth element is the context.
        if self.related_entities_history:
            query_dataset = f"""
                SELECT DISTINCT ?s ?p ?o ?c
                WHERE {{
                    {{BIND (<{self.res}> AS ?s)}}
                    UNION 
                    {{BIND (<{self.res}> AS ?o)}}
                    GRAPH ?c {{?s ?p ?o}}
                }}   
            """
        else:
            query_dataset = f"""
                SELECT DISTINCT ?s ?p ?o ?c
                WHERE {{
                    BIND (<{self.res}> AS ?s) 
                    GRAPH ?c {{?s ?p ?o}}
                }}   
            """
        return Sparql(query_dataset, config_path=self.config_path).run_construct_query()

    def _query_provenance(self, include_prov_metadata:bool=False) -> ConjunctiveGraph:
        if include_prov_metadata:
            query_provenance = f"""
                CONSTRUCT {{
                    ?snapshot <{ProvEntity.iri_generated_at_time}> ?t; 
                              <{ProvEntity.iri_was_attributed_to}> ?responsibleAgent;
                              <{ProvEntity.iri_had_primary_source}> ?source;
                              <{ProvEntity.iri_description}> ?description;
                              <{ProvEntity.iri_has_update_query}> ?updateQuery.
                }} 
                WHERE {{
                    ?snapshot <{ProvEntity.iri_specialization_of}> <{self.res}>;
                              <{ProvEntity.iri_was_attributed_to}> ?responsibleAgent;
                              <{ProvEntity.iri_generated_at_time}> ?t;
                              <{ProvEntity.iri_description}> ?description.
                    OPTIONAL {{ ?snapshot <{ProvEntity.iri_had_primary_source}> ?source. }}   
                    OPTIONAL {{ ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery. }}  
                }}
            """
        else:
            query_provenance = f"""
                CONSTRUCT {{
                    ?snapshot <{ProvEntity.iri_generated_at_time}> ?t;      
                              <{ProvEntity.iri_has_update_query}> ?updateQuery.
                }} 
                WHERE {{
                    ?snapshot <{ProvEntity.iri_specialization_of}> <{self.res}>;
                              <{ProvEntity.iri_generated_at_time}> ?t.
                    OPTIONAL {{ ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery. }}   
                }}
            """
        return Sparql(query_provenance, config_path=self.config_path).run_construct_query()

def _get_entities_histories(res_set: Set[str], include_prov_metadata:bool=False, config_path:str=".config.json") -> Tuple[Dict[str, Dict[str, ConjunctiveGraph]], Dict]:
    entities_histories = [dict(), dict()]
    for res in res_set:
        agnosticEntity = AgnosticEntity(res, related_entities_history=False, config_path=config_path)
        history_and_metadata = agnosticEntity.get_history(include_prov_metadata=include_prov_metadata)
        if history_and_metadata:
            history = history_and_metadata[0]
            entities_histories[0].update(history)
        if include_prov_metadata:
            metadata = history_and_metadata[1]
            entities_histories[1].update(metadata)
    return tuple(entities_histories)

def _filter_timestamps_by_interval(interval:Tuple[str, str], iterator:list, time_index:int=None) -> set:
    if interval:
        after_time = convert_to_datetime(interval[0])
        before_time = convert_to_datetime(interval[1])
        relevant_timestamps = set()
        for timestamp in iterator:
            time = convert_to_datetime(timestamp[time_index]) if time_index is not None else convert_to_datetime(timestamp)
            if after_time and before_time:
                if time >= after_time and time <= before_time:
                    relevant_timestamps.add(timestamp)
            if after_time and not before_time:
                if time >= after_time:
                    relevant_timestamps.add(timestamp)
            if before_time and not after_time:
                if time <= before_time:
                    relevant_timestamps.add(timestamp)
    else:
        relevant_timestamps = set(timestamp for timestamp in iterator)
    return relevant_timestamps