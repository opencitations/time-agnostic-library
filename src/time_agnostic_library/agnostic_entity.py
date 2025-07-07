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

import copy
from typing import Dict, List, Set, Tuple, Union

from rdflib import RDF, BNode, Graph, Literal, Namespace, URIRef
from rdflib.graph import ConjunctiveGraph, Graph
from rdflib.namespace import NamespaceManager
from rdflib.plugins.sparql import parser
from rdflib.term import BNode, URIRef
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
    :param include_related_objects: True, if you also want to return information on related entities, those that have the URI of the res parameter as an object recursively, False otherwise.
    :type include_related_objects: bool, optional
    :param include_merged_entities: True, if you also want to return information on entities that were merged into the current entity, False otherwise.
    :type include_merged_entities: bool, optional
    :param include_reverse_relations: True, if you also want to return information on entities that have the current entity as an object recursively, False otherwise.
    :type include_reverse_relations: bool, optional
    :param config: The configuration dictionary.
    :type config: dict
    """

    def __init__(self, res:str, config:dict, include_related_objects:bool=False, include_merged_entities:bool=False, include_reverse_relations:bool=False):
        self.res = res
        self.include_related_objects = include_related_objects
        self.include_merged_entities = include_merged_entities
        self.include_reverse_relations = include_reverse_relations
        self.config = config

    def get_history(self, include_prov_metadata: bool=False) -> Tuple[Dict[str, Dict[str, Graph]], Dict[str, Dict[str, Dict[str, str]]]]:
        """
        It materializes all versions of an entity. If any of the include_* parameters are True, 
        it also materializes all versions of related entities based on the configured parameters:
        - include_related_objects: entities that have **res** as subject (recursively)
        - include_merged_entities: entities that were merged into **res**
        - include_reverse_relations: entities that have **res** as object (recursively)
        
        If **include_prov_metadata** is True, 
        the provenance metadata of the returned entity/entities is also returned.

        The output is a tuple where the first element is a dictionary mapping timestamps to merged graphs,
        and the second element is a dictionary containing provenance metadata if requested.
        
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
        if self.include_related_objects or self.include_merged_entities or self.include_reverse_relations:
            histories = {}
            self._collect_all_related_entities_histories(histories, include_prov_metadata)
            return self._get_merged_histories(histories, include_prov_metadata)
        else:
            entity_history = self._get_entity_current_state(include_prov_metadata)
            entity_history = self._get_old_graphs(entity_history)
            return tuple(entity_history)

    def _collect_all_related_entities_histories(
        self, 
        histories: Dict[str, Tuple[Dict[str, Graph], Dict[str, Dict[str, str]]]], 
        include_prov_metadata: bool
    ) -> None:
        """
        Collects histories for all related entities using three separate recursions.
        """
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
        processed_entities: Set[str], 
        histories: Dict[str, Tuple[Dict[str, Graph], Dict[str, Dict[str, str]]]], 
        include_prov_metadata: bool, 
        depth: int = None
    ) -> None:
        """
        Recursively collects entities that are objects of the given entity.
        """
        if depth is not None and depth <= 0:
            return

        next_depth = None if depth is None else depth - 1

        entity_graphs = histories[entity_uri][0][entity_uri] if entity_uri in histories else None
        if not entity_graphs:
            return

        related_objects = set()
        for timestamp, graph in entity_graphs.items():
            for triple in graph.triples((URIRef(entity_uri), None, None)):
                predicate = triple[1]
                obj = triple[2]
                if isinstance(obj, URIRef) and ProvEntity.PROV not in predicate and predicate != RDF.type:
                    obj_uri = str(obj)
                    related_objects.add(obj_uri)

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
        processed_entities: Set[str], 
        histories: Dict[str, Tuple[Dict[str, Graph], Dict[str, Dict[str, str]]]], 
        include_prov_metadata: bool, 
        depth: int = None
    ) -> None:
        """
        Recursively collects entities that were merged into the given entity.
        """
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
        processed_entities: Set[str], 
        histories: Dict[str, Tuple[Dict[str, Graph], Dict[str, Dict[str, str]]]], 
        include_prov_metadata: bool, 
        depth: int = None
    ) -> None:
        """
        Recursively collects entities that have the given entity as an object.
        """
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
        histories: Dict[str, Tuple[Dict[str, Dict[str, Graph]], Union[Dict[str, Dict[str, str]], None]]], 
        include_prov_metadata: bool
    ) -> Tuple[Dict[str, Graph], Dict[str, Dict[str, Dict[str, str]]]]:
        """
        Merges the histories of the main entity and related entities based on timestamps.

        :param histories: A dictionary containing the histories and metadata of entities.
        :param include_prov_metadata: Whether to include provenance metadata.
        :return: A tuple of merged histories and metadata.
        """
        entity_histories = {}

        metadata = {}
        for entity_uri, (entity_history_dict, entity_metadata) in histories.items():
            entity_histories[entity_uri] = entity_history_dict[entity_uri]
            if include_prov_metadata and entity_metadata:
                metadata[entity_uri] = entity_metadata[entity_uri]

        main_entity_times = sorted(
            entity_histories[self.res].keys(), key=lambda x: convert_to_datetime(x)
        )

        merged_histories = {self.res: {}}

        for timestamp in main_entity_times:
            merged_graph = copy.deepcopy(entity_histories[self.res][timestamp])

            for entity_uri in entity_histories:
                if entity_uri == self.res:
                    continue
                entity_times = sorted(
                    entity_histories[entity_uri].keys(), key=lambda x: convert_to_datetime(x)
                )

                relevant_time = None
                for etime in entity_times:
                    if convert_to_datetime(etime) <= convert_to_datetime(timestamp):
                        relevant_time = etime
                    else:
                        break
                if relevant_time:
                    for quad in entity_histories[entity_uri][relevant_time].quads():
                        merged_graph.add(quad)

            merged_histories[self.res][timestamp] = merged_graph

        return merged_histories, metadata

    def get_state_at_time(
        self, 
        time: Tuple[Union[str, None]], 
        include_prov_metadata: bool = False, 
        ) -> Tuple[Graph, dict, Union[dict, None]]:
        """
        Given a time interval, the function returns the states of the resource and optionally its related entities within the interval,
        the returned snapshots metadata, and optionally, the hooks to the previous and subsequent snapshots.

        Related entities are included based on the configured parameters:
        - include_related_objects: entities that have **res** as subject (recursively)
        - include_merged_entities: entities that were merged into **res**
        - include_reverse_relations: entities that have **res** as object (recursively)

        The output has the following format:

        (
            {
                ENTITY_URI_1: {
                    TIME_1: GRAPH_AT_TIME_1, 
                    TIME_2: GRAPH_AT_TIME_2
                },
                ENTITY_URI_2: {
                    TIME_1: GRAPH_AT_TIME_1, 
                    TIME_2: GRAPH_AT_TIME_2
                },
                ...
            },
            {
                ENTITY_URI_1: {
                    SNAPSHOT_URI_AT_TIME_1: METADATA,
                    SNAPSHOT_URI_AT_TIME_2: METADATA
                },
                ENTITY_URI_2: {
                    SNAPSHOT_URI_AT_TIME_1: METADATA,
                    SNAPSHOT_URI_AT_TIME_2: METADATA
                },
                ...
            },
            {
                ENTITY_URI_1: {
                    OTHER_SNAPSHOT_URI_1: METADATA,
                    OTHER_SNAPSHOT_URI_2: METADATA
                },
                ENTITY_URI_2: {
                    OTHER_SNAPSHOT_URI_1: METADATA,
                    OTHER_SNAPSHOT_URI_2: METADATA
                },
                ...
            }
        )

        :param time: A time interval, in the form (START, END). If one of the two values is None, only the other is considered. The time can be specified using any existing standard.
        :type time: Tuple[Union[str, None]].
        :param include_prov_metadata: If True, hooks are returned to the previous and subsequent snapshots.
        :type include_prov_metadata: bool, optional
        :returns: Tuple[Dict[str, Dict[str, Graph]], Dict[str, Dict[str, Dict[str, str]]], Union[Dict[str, Dict[str, Dict[str, str]]], None]] -- The method returns a tuple of three elements: the first is a dictionary mapping entity URIs to their graphs at timestamps within the specified interval; the second contains the snapshots metadata of the states that have been returned; if the **include_prov_metadata** parameter is True, the third element of the tuple is the metadata on the other snapshots, otherwise an empty dictionary.
        """
        if self.include_related_objects or self.include_merged_entities or self.include_reverse_relations:
            histories = {}
            self._collect_all_related_entities_states_at_time(histories, time, include_prov_metadata)
            return self._get_merged_histories_at_time(histories, include_prov_metadata)
        else:
            return self._get_entity_state_at_time(time, include_prov_metadata)

    def _collect_all_related_entities_states_at_time(
        self, 
        histories: Dict[str, Tuple[Dict[str, Graph], Dict[str, Dict[str, str]], Union[Dict[str, Dict[str, Dict[str, str]]], None]]], 
        time: Tuple[Union[str, None]], 
        include_prov_metadata: bool
    ) -> None:
        """
        Collects states at time for all related entities using three separate recursions.
        """
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
        processed_entities: Set[str], 
        histories: Dict[str, Tuple[Dict[str, Graph], Dict[str, Dict[str, str]], Union[Dict[str, Dict[str, Dict[str, str]]], None]]], 
        time: Tuple[Union[str, None]], 
        include_prov_metadata: bool, 
        depth: int = None
    ) -> None:
        """
        Recursively collects entity states at time for entities that are objects of the given entity.
        """
        if depth is not None and depth <= 0:
            return

        next_depth = None if depth is None else depth - 1

        entity_graphs = histories[entity_uri][0] if entity_uri in histories else None
        if not entity_graphs:
            return

        related_objects = set()
        for timestamp, graph in entity_graphs.items():
            for triple in graph.triples((URIRef(entity_uri), None, None)):
                predicate = triple[1]
                obj = triple[2]
                if isinstance(obj, URIRef) and ProvEntity.PROV not in predicate and predicate != RDF.type:
                    obj_uri = str(obj)
                    related_objects.add(obj_uri)

        for obj_uri in related_objects:
            if obj_uri not in processed_entities:
                processed_entities.add(obj_uri)
                
                agnostic_entity = AgnosticEntity(obj_uri, self.config, include_related_objects=False, include_merged_entities=False, include_reverse_relations=False)
                entity_graphs, entity_snapshots, other_snapshots_metadata = agnostic_entity._get_entity_state_at_time(time, include_prov_metadata)
                histories[obj_uri] = (entity_graphs, entity_snapshots, other_snapshots_metadata)
                
                self._collect_related_objects_states_at_time(obj_uri, processed_entities, histories, time, include_prov_metadata, next_depth)

    def _collect_merged_entities_states_at_time(
        self, 
        entity_uri: str, 
        processed_entities: Set[str], 
        histories: Dict[str, Tuple[Dict[str, Graph], Dict[str, Dict[str, str]], Union[Dict[str, Dict[str, Dict[str, str]]], None]]], 
        time: Tuple[Union[str, None]], 
        include_prov_metadata: bool, 
        depth: int = None
    ) -> None:
        """
        Recursively collects entity states at time for entities that were merged into the given entity.
        """
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
        processed_entities: Set[str], 
        histories: Dict[str, Tuple[Dict[str, Graph], Dict[str, Dict[str, str]], Union[Dict[str, Dict[str, Dict[str, str]]], None]]], 
        time: Tuple[Union[str, None]], 
        include_prov_metadata: bool, 
        depth: int = None
    ) -> None:
        """
        Recursively collects entity states at time for entities that have the given entity as an object.
        """
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
        histories: Dict[str, Tuple[Dict[str, Graph], Dict[str, Dict[str, str]], Union[Dict[str, Dict[str, Dict[str, str]]], None]]], 
        include_prov_metadata: bool
    ) -> Tuple[Dict[str, Dict[str, Graph]], Dict[str, Dict[str, Dict[str, str]]], Union[Dict[str, Dict[str, Dict[str, str]]], None]]:
        """
        Merges the states of the main entity and related entities at the given times.

        :param histories: A dictionary containing the states and metadata of entities.
        :param include_prov_metadata: Whether to include provenance metadata.
        :return: A tuple of merged histories and metadata.
        """
        entity_histories = {}
        entity_snapshots_metadata = {}
        other_snapshots_metadata = {} if include_prov_metadata else None

        for entity_uri, (entity_graphs, entity_snapshots, other_snapshots) in histories.items():
            entity_histories[entity_uri] = entity_graphs
            entity_snapshots_metadata[entity_uri] = entity_snapshots
            if include_prov_metadata and other_snapshots:
                other_snapshots_metadata[entity_uri] = other_snapshots

        main_entity_times = sorted(
            set(entity_histories[self.res].keys()), key=lambda x: convert_to_datetime(x)
        )

        merged_histories = {self.res: {}}

        for timestamp in main_entity_times:
            merged_graph = copy.deepcopy(entity_histories[self.res][timestamp])

            for entity_uri, graphs_at_times in entity_histories.items():
                if entity_uri == self.res:
                    continue
                if timestamp in graphs_at_times:
                    related_graph = graphs_at_times[timestamp]
                else:
                    related_times = sorted(
                        graphs_at_times.keys(), key=lambda x: convert_to_datetime(x)
                    )
                    relevant_time = None
                    for rt in related_times:
                        if convert_to_datetime(rt) <= convert_to_datetime(timestamp):
                            relevant_time = rt
                        else:
                            break
                    if relevant_time:
                        related_graph = graphs_at_times[relevant_time]
                    else:
                        continue
                for quad in related_graph.quads((None, None, None, None)):
                    merged_graph.add(quad)

            merged_histories[self.res][timestamp] = merged_graph

        return merged_histories, entity_snapshots_metadata, other_snapshots_metadata

    def _get_entity_state_at_time(
        self, 
        time: Tuple[Union[str, None]], 
        include_prov_metadata: bool
    ) -> Tuple[Dict[str, Graph], Dict[str, Dict[str, str]], Union[Dict[str, Dict[str, str]], None]]:
        """
        Get the state of the entity at the given time(s).
        
        :param time: A time interval.
        :param include_prov_metadata: Whether to include provenance metadata.
        :return: A tuple containing the entity graphs at times, snapshots metadata, and other snapshots metadata.
        """
        other_snapshots_metadata = {}
        is_quadstore = self.config["provenance"]["is_quadstore"]
        graph_statement = f"GRAPH <{self.res}/prov/>" if is_quadstore else ""
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
        results = Sparql(query_snapshots, config=self.config).run_select_query()
        bindings = results['results']['bindings']
        if not bindings:
            return {}, {}, other_snapshots_metadata
        sorted_results = sorted(bindings, key=lambda x: convert_to_datetime(x['time']['value']), reverse=True)
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
            interval_start = convert_to_datetime(time[0]) if time[0] else None
            if interval_start:
                earlier_snapshots = [r for r in bindings if convert_to_datetime(r['time']['value']) <= interval_start]
                if earlier_snapshots:
                    latest_snapshot = max(earlier_snapshots, key=lambda x: convert_to_datetime(x['time']['value']))
                    relevant_results = [latest_snapshot]
                else:
                    return {}, {}, other_snapshots_metadata
            else:
                return {}, {}, other_snapshots_metadata
        entity_snapshots = {}
        entity_graphs = {}
        if not relevant_results:
            return entity_graphs, entity_snapshots, other_snapshots_metadata
        entity_cg = self._query_dataset(self.res)
        for relevant_result in relevant_results:
            sum_update_queries = ""
            relevant_result_time = relevant_result['time']['value']
            for result in bindings:
                result_time = result['time']['value']
                if 'updateQuery' in result and 'value' in result['updateQuery']:
                    if convert_to_datetime(result_time) > convert_to_datetime(relevant_result_time):
                        if sum_update_queries:
                            sum_update_queries += ";"
                        sum_update_queries += result['updateQuery']['value']
            entity_present_graph = copy.deepcopy(entity_cg)
            if sum_update_queries:
                self._manage_update_queries(entity_present_graph, sum_update_queries)
            timestamp_key = convert_to_datetime(relevant_result_time, stringify=True)
            entity_graphs[timestamp_key] = entity_present_graph
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
    
    def _include_prov_metadata(self, triples_generated_at_time:list, current_state:ConjunctiveGraph) -> dict:
        if list(current_state.triples((URIRef(self.res), URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), ProvEntity.iri_entity))): 
            return dict()
        prov_properties = {
            ProvEntity.iri_invalidated_at_time: "invalidatedAtTime",
            ProvEntity.iri_was_attributed_to: "wasAttributedTo", 
            ProvEntity.iri_had_primary_source: "hadPrimarySource", 
            ProvEntity.iri_description: "description",
            ProvEntity.iri_has_update_query: "hasUpdateQuery",
            ProvEntity.iri_was_derived_from: "wasDerivedFrom"
        }
        prov_metadata = {
            self.res: dict()
        }
        for triple in triples_generated_at_time:
            time = convert_to_datetime(triple[2], stringify=True)
            snapshot_uri_str = str(triple[0])
            prov_metadata[self.res][snapshot_uri_str] = {
                "generatedAtTime": time,
                "invalidatedAtTime": None,
                "wasAttributedTo": None,
                "hadPrimarySource": None,
                "description": None,
                "hasUpdateQuery": None,
                "wasDerivedFrom": []
            }
        for entity, metadata in dict(prov_metadata).items():
            for se_uri_str, snapshot_data in metadata.items():
                se_uri = URIRef(se_uri_str)
                for prov_property, abbr in prov_properties.items():
                    triples_with_property = list(current_state.triples((se_uri, prov_property, None)))
                    for triple in triples_with_property:
                        value = str(triple[2])
                        if abbr == "wasDerivedFrom":
                            if not isinstance(snapshot_data[abbr], list):
                                snapshot_data[abbr] = []
                            snapshot_data[abbr].append(value)
                        else:
                            snapshot_data[abbr] = value
                
                if "wasDerivedFrom" in snapshot_data and isinstance(snapshot_data["wasDerivedFrom"], list):
                    snapshot_data["wasDerivedFrom"] = sorted(snapshot_data["wasDerivedFrom"])
                            
        return prov_metadata

    def _get_entity_current_state(self, include_prov_metadata: bool = False) -> list:
        entity_current_state = [{self.res: dict()}]
        current_state = ConjunctiveGraph()
        for quad in self._query_provenance(include_prov_metadata).quads():
            current_state.add(quad)
        if len(current_state) == 0:
            entity_current_state.append(dict())
            return entity_current_state
        for quad in self._query_dataset(self.res).quads():
            current_state.add(quad)
        triples_generated_at_time = list(
            current_state.triples((None, ProvEntity.iri_generated_at_time, None))
        )
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
            prov_metadata = self._include_prov_metadata(
                triples_generated_at_time, current_state
            )
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
                snapshot_uri = previous_graph.value(
                    predicate=ProvEntity.iri_generated_at_time,
                    object=Literal(next_snapshot)
                )
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
            time_str = convert_to_datetime(time, stringify=True)
            entity_current_state[0][self.res][time_str] = cg_no_pro
        return entity_current_state

    @classmethod
    def _manage_update_queries(cls, graph: ConjunctiveGraph, update_query: str) -> None:
        def extract_namespaces(parsed_query):
            namespace_manager = NamespaceManager(Graph())
            if hasattr(parsed_query, 'prologue') and parsed_query.prologue:
                for prefix_decl in parsed_query.prologue[0]:
                    if hasattr(prefix_decl, 'prefix') and hasattr(prefix_decl, 'iri'):
                        prefix = prefix_decl.prefix
                        iri = prefix_decl.iri
                        namespace_manager.bind(prefix, Namespace(iri))
            return namespace_manager

        def extract_quads_from_update(parsed_query, namespace_manager):
            operations = []
            
            for update_request in parsed_query.request:
                operation_type = update_request.name
                if operation_type in ['DeleteData', 'InsertData']:
                    quads = extract_quads(update_request, namespace_manager)
                    operations.append((operation_type, quads))
            
            return operations

        def extract_quads(operation, namespace_manager):
            quads = []
            if hasattr(operation.quads, 'quadsNotTriples'):
                for quad_not_triple in operation.quads.quadsNotTriples:
                    context = quad_not_triple.term
                    for triple in quad_not_triple.triples:
                        s, p, o = triple
                        s = _process_node(s, namespace_manager)
                        p = _process_node(p, namespace_manager)
                        o = _process_node(o, namespace_manager)
                        quads.append((s, p, o, context))
            return quads

        def _process_node(node, namespace_manager: NamespaceManager):
            if isinstance(node, dict):
                if 'prefix' in node and 'localname' in node:
                    namespace = namespace_manager.store.namespace(node['prefix'])
                    return URIRef(f"{namespace}{node['localname']}")
                elif 'string' in node:
                    if 'datatype' in node:
                        return Literal(node['string'], datatype=node['datatype'])
                    elif 'lang' in node:
                        return Literal(node['string'], lang=node['lang'])
                    else:
                        return Literal(node['string'])
            elif isinstance(node, BNode):
                return node
            return URIRef(node)

        def match_literal(graph_literal, query_literal):
            if isinstance(graph_literal, Literal) and isinstance(query_literal, Literal):
                # Check datatype compatibility first
                datatypes_match = (graph_literal.datatype == query_literal.datatype or query_literal.datatype is None)
                
                # Try value comparison, fallback to string comparison if values are None
                values_match = False
                graph_value = graph_literal.value
                query_value = query_literal.value
                
                if graph_value is not None and query_value is not None:
                    values_match = graph_value == query_value
                elif graph_value is None and query_value is None:
                    # Both values are None (like xsd:gYear), compare string representations
                    values_match = str(graph_literal) == str(query_literal)
                else:
                    # One is None, one is not - no match
                    values_match = False
                
                return values_match and datatypes_match
            return graph_literal == query_literal

        try:
            parsed_query = parser.parseUpdate(update_query)
            namespace_manager = extract_namespaces(parsed_query)
            operations = extract_quads_from_update(parsed_query, namespace_manager)
            
            for operation_type, quads in operations:
                if operation_type == 'DeleteData':
                    for quad in quads:
                        graph.add(quad)
                elif operation_type == 'InsertData':
                    for s, p, o, c in quads:
                        quads_to_remove = []
                        for graph_quad in graph.quads((s, p, None, c)):
                            if match_literal(graph_quad[2], o):
                                quads_to_remove.append(graph_quad)
                            else:
                                quads_to_remove.append((s, p, o, c))
                        for quad in quads_to_remove:
                            graph.remove(quad)
        
        except Exception as e:
            print(f"Error processing update query: {e}")
            print(f"Problematic query: {update_query}")

    def _query_dataset(self, entity_uri: str = None) -> ConjunctiveGraph:
        # A SELECT hack can be used to return RDF quads in named graphs,
        # since the CONSTRUCT allows only to return triples in SPARQL 1.1.
        # Here is an example of SELECT hack using VALUES:
        #
        # SELECT ?s ?p ?o ?c
        # WHERE {
        #     VALUES ?s {<resource_uri>}
        #     GRAPH ?c {?s ?p ?o}
        # }
        #
        # Afterwards, the rdflib add method can be used to add quads to a Conjunctive Graph,
        # where the fourth element is the context.
        
        entity_uri = self.res if entity_uri is None else entity_uri

        is_quadstore = self.config['dataset']['is_quadstore']

        if is_quadstore:
            query_dataset = f"""
                SELECT DISTINCT ?s ?p ?o ?g
                WHERE {{
                    GRAPH ?g {{
                        VALUES ?s {{<{entity_uri}>}}
                        ?s ?p ?o
                    }}
                }}   
            """
        else:
            query_dataset = f"""
                SELECT DISTINCT ?s ?p ?o
                WHERE {{
                    VALUES ?s {{<{entity_uri}>}}
                    ?s ?p ?o
                }}   
            """

        return Sparql(query_dataset, config=self.config).run_construct_query()

    def _query_provenance(self, include_prov_metadata:bool=False) -> ConjunctiveGraph:
        if include_prov_metadata:
            query_provenance = f"""
                CONSTRUCT {{
                    ?snapshot <{ProvEntity.iri_generated_at_time}> ?t; 
                              <{ProvEntity.iri_was_attributed_to}> ?responsibleAgent;
                              <{ProvEntity.iri_had_primary_source}> ?source;
                              <{ProvEntity.iri_description}> ?description;
                              <{ProvEntity.iri_has_update_query}> ?updateQuery;
                              <{ProvEntity.iri_invalidated_at_time}> ?invalidatedAtTime;
                              <{ProvEntity.iri_was_derived_from}> ?derived_from_snapshot.
                }} 
                WHERE {{
                    ?snapshot <{ProvEntity.iri_specialization_of}> <{self.res}>;
                              <{ProvEntity.iri_was_attributed_to}> ?responsibleAgent;
                              <{ProvEntity.iri_generated_at_time}> ?t;
                              <{ProvEntity.iri_description}> ?description.
                    OPTIONAL {{ ?snapshot <{ProvEntity.iri_had_primary_source}> ?source. }}   
                    OPTIONAL {{ ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery. }}
                    OPTIONAL {{ ?snapshot <{ProvEntity.iri_invalidated_at_time}> ?invalidatedAtTime. }}  
                    OPTIONAL {{ ?snapshot <{ProvEntity.iri_was_derived_from}> ?derived_from_snapshot. }}
                }}
            """
        else:
            query_provenance = f"""
                CONSTRUCT {{
                    ?snapshot <{ProvEntity.iri_generated_at_time}> ?t;      
                              <{ProvEntity.iri_has_update_query}> ?updateQuery;
                              <{ProvEntity.iri_was_derived_from}> ?derived_from_snapshot.
                }} 
                WHERE {{
                    ?snapshot <{ProvEntity.iri_specialization_of}> <{self.res}>;
                              <{ProvEntity.iri_generated_at_time}> ?t.
                    OPTIONAL {{ ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery. }}
                    OPTIONAL {{ ?snapshot <{ProvEntity.iri_was_derived_from}> ?derived_from_snapshot. }}
                }}
            """
        return Sparql(query_provenance, config=self.config).run_construct_query()

    def _find_merged_entities(self, entity_uri: str) -> Set[str]:
        """
        Finds entities that were merged into the given entity_uri based on provenance.
        An entity is considered merged if one of its snapshots was used in a prov:wasDerivedFrom
        relation by a snapshot of entity_uri.
        """
        merged_entity_uris = set()
        query_simple = f"""
            SELECT DISTINCT ?merged_entity_uri
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

    def _find_reverse_related_entities(self, entity_uri: str) -> Set[str]:
        """
        Finds entities that have the given entity_uri as an object recursively.
        Returns the URIs of entities that have relationships pointing to entity_uri.
        """
        reverse_related_entity_uris = set()
        
        is_quadstore = self.config['dataset']['is_quadstore']

        if is_quadstore:
            query = f"""
                SELECT DISTINCT ?subject
                WHERE {{
                    GRAPH ?g {{
                        ?subject ?predicate <{entity_uri}> .
                        FILTER(?predicate != <{RDF.type}> && !strstarts(str(?predicate), "{ProvEntity.PROV}"))
                    }}
                }}   
            """
        else:
            query = f"""
                SELECT DISTINCT ?subject
                WHERE {{
                    ?subject ?predicate <{entity_uri}> .
                    FILTER(?predicate != <{RDF.type}> && !strstarts(str(?predicate), "{ProvEntity.PROV}"))
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

def _filter_timestamps_by_interval(interval: Tuple[str, str], iterator: list, time_index: str = None) -> list:
    if interval:
        after_time = convert_to_datetime(interval[0]) if interval[0] else None
        before_time = convert_to_datetime(interval[1]) if interval[1] else None
        relevant_timestamps = []
        for timestamp in iterator:
            if time_index is not None and time_index in timestamp:
                time_binding = timestamp[time_index]
                if 'value' in time_binding:
                    time_str = time_binding['value']
                    time = convert_to_datetime(time_str)
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