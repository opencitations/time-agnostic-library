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

from typing import List, Tuple, Dict, Set
import copy, re
from datetime import datetime
from rdflib.graph import ConjunctiveGraph
from rdflib.term import URIRef
from rdflib.plugins.sparql.processor import processUpdate
from dateutil import parser

from time_agnostic_library.sparql import Sparql
from time_agnostic_library.prov_entity import ProvEntity


class AgnosticEntity:
    """
    An entity you want to obtain information about.
    Not only the present ones, but also the past ones, 
    based on the snapshots available for that entity.

    :param res: The URI of the entity.
    :type res: str.
    :param related_entities_history: True if you also want to return information on related entities, that is, the ones that have the URI of the res parameter as object, False otherwise. The default is False. 
    :type related_entities_history: bool.
    """

    def __init__(self, res: str, related_entities_history: bool = False):
        self.res = res
        self.related_entities_history = related_entities_history

    def get_history(self, include_prov_metadata:bool=False) -> Tuple[Dict[str, Dict[str, ConjunctiveGraph]], Dict[URIRef, Dict[str, Dict[str, str]]]]:
        """
        Given the URI of a resource, it reconstructs its entire history, 
        returning a dictionary according to the following model: ::

            {
                "entity_URI": {
                    "time_snapshot_1": graph_at_time_1,
                    "time_snapshot_2": graph_at_time_2,
                    "time_snapshot_n": graph_at_time_n
                }
            }

        What is meant by graph? By graph we mean all the triples that have 
        the entity as subject plus the provenance information present at time t 
        (not all existing ones, only those existing at that given instant).
        If the object of type AgnosticEntity has been instantiated by passing the 
        related_entities_history parameter as True, the graph also contains the related entities, 
        that is, those that have the entity as an object.

        :returns:  Dict[str, Dict[str, ConjunctiveGraph]] -- A dictionary containing the graphs related to each considered entities in each of the existing snapshots of these entities.
        """
        if self.related_entities_history:
            entities_to_query = {self.res}
            current_state = self._query_dataset()
            related_entities = current_state.triples(
                (None, None, URIRef(self.res)))
            for entity in related_entities:
                if ProvEntity.PROV not in entity[1]:
                    entities_to_query.add(str(entity[0]))
            return get_entities_histories(entities_to_query, include_prov_metadata)
        entity_history = self._get_entity_current_state(include_prov_metadata)
        entity_history = self._get_old_graphs(entity_history)
        return tuple(entity_history)

    def get_state_at_time(
        self, 
        time: str, 
        include_prov_metadata: bool = False, 
        ) -> Tuple[ConjunctiveGraph, Dict[str, str], Dict[str, str]]:
        """
        Given a time, the function returns the resource's state at that time, the returned snapshot metadata
        and, optionally, the hooks to the previous and subsequent states.
        Snapshot metadata includes generation time, the responsible agent and the primary source.
        The specified time can be any time, not necessarily the exact time of a snapshot. 
        In addition, it can be specified in any existing standard. 
        Here is an example of possible output: ::

            (
                <Graph identifier=N75add58bd55b4d9f88082787e3e2c888 (<class 'rdflib.graph.ConjunctiveGraph'>)>,
                {
                    'https://github.com/arcangelo7/time_agnostic/id/1/prov/se/2': {
                        'http://www.w3.org/ns/prov#generatedAtTime': '2021-06-04T09:36:53.000Z',
                        'http://www.w3.org/ns/prov#hadPrimarySource': None,
                        'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696'
                    }
                },
                {
                    'https://github.com/arcangelo7/time_agnostic/id/1/prov/se/1': {
                        'http://www.w3.org/ns/prov#generatedAtTime': '2021-05-29T16:20:22.000Z',
                        'http://www.w3.org/ns/prov#hadPrimarySource': None,
                        'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696'
                    },
                    'https://github.com/arcangelo7/time_agnostic/id/1/prov/se/3': {
                        'http://www.w3.org/ns/prov#generatedAtTime': '2021-06-04T11:15:32.000Z',
                        'http://www.w3.org/ns/prov#hadPrimarySource': None,
                        'http://www.w3.org/ns/prov#wasAttributedTo': 'https://orcid.org/0000-0002-8420-0696'
                    }
                }
            )

        :param time: Any time value, not necessarily the exact value of a snapshot. The status of the resource will be returned to the most recent past snapshot compared to the specified time. The time can be specified using any existing standard.
        :type time: str.
        :param include_prov_metadata: If True, hooks are returned to the previous and subsequent snapshots. The default value is False.
        :type include_prov_metadata: bool.
        :returns: tuple --   The method always returns a tuple of three elements: the first is the resource conjunctive graph at that time, the second is the snapshot metadata of which the state has been returned. If the include_prov_metadata parameter is True, the third element of the tuple is the metadata on the other snapshots, otherwise an empty dictionary. The third dictionary is empty also if only one snapshot exists.
        """
        datetime_time = self._convert_to_datetime(time)
        query_snapshots = f"""
            SELECT ?snapshot ?time ?responsibleAgent ?updateQuery ?primarySource
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
        results = list(Sparql(query_snapshots).run_select_query())
        results.sort(key=lambda x:self._convert_to_datetime(x[1]), reverse=True)
        results_after_time = list()
        for result in results:
            if self._convert_to_datetime(result[1]) > datetime_time:
                results_after_time.append(result)
        sum_update_queries = ""
        for result in results_after_time:
            if result[3] is not None:
                sum_update_queries += result[3] + ";"
        entity_cg = self._query_dataset()
        self._manage_update_queries(entity_cg, sum_update_queries)
        entity_snapshot = dict()
        snapshot_to_return = min(results, key=lambda x:abs(self._convert_to_datetime(x[1])-datetime_time))
        entity_snapshot[snapshot_to_return[0]] = {
            str(ProvEntity.iri_generated_at_time): snapshot_to_return[1],
            str(ProvEntity.iri_was_attributed_to): snapshot_to_return[2],
            str(ProvEntity.iri_had_primary_source): snapshot_to_return[4]
        }
        if include_prov_metadata:
            results.remove(snapshot_to_return)
            other_snapshots = dict()
            for result_tuple in results:
                other_snapshots[result_tuple[0]] = {
                    str(ProvEntity.iri_generated_at_time): result_tuple[1],
                    str(ProvEntity.iri_was_attributed_to): result_tuple[2],
                    str(ProvEntity.iri_had_primary_source): result_tuple[4]
            }
            return entity_cg, entity_snapshot, other_snapshots
        return entity_cg, entity_snapshot, None
    
    def _include_prov_metadata(self, triples_generated_at_time:list, current_state:ConjunctiveGraph) -> dict:
        prov_properties = [
            ProvEntity.iri_was_attributed_to, 
            ProvEntity.iri_had_primary_source, 
            ProvEntity.iri_description
        ] 
        prov_metadata = {
            self.res: dict()
        }
        for triple in triples_generated_at_time:
            time = self._convert_to_datetime(triple[2]).strftime("%Y-%m-%dT%H:%M:%S")
            prov_metadata[self.res][str(triple[0])] = {
                "http://www.w3.org/ns/prov#generatedAtTime": time,
                "http://www.w3.org/ns/prov#wasAttributedTo": None,
                "http://www.w3.org/ns/prov#hadPrimarySource": None,
                "http://purl.org/dc/terms/description": None
            }
        for entity, metadata in dict(prov_metadata).items():
            for se, _ in metadata.items():
                for prov_property in prov_properties:
                    triples_with_property = current_state.triples(
                    (URIRef(se), prov_property, None))
                    for triple in triples_with_property:
                        prov_metadata[entity][str(triple[0])][str(triple[1])] = str(triple[2])
        return prov_metadata

    def _get_entity_current_state(self, include_prov_metadata:bool=False) -> list:
        """
        Given the URI of a resource, it outputs a dictionary populating only 
        the instant at time t, that is the present one, according to the following model:
        {
            "entity_URI": {
                "time_snapshot_1": graph_at_time_1,
                "time_snapshot_2": None,
                "time_snapshot_n": None
            }
        }        
        """
        entity_current_state = [{self.res: dict()}]
        current_state = ConjunctiveGraph()
        for quad in self._query_dataset().quads():
            current_state.add(quad)
        for quad in self._query_provenance(include_prov_metadata).quads():
            current_state.add(quad)
        if len(current_state) == 0:
            entity_current_state.append(None)
            return entity_current_state
        triples_generated_at_time = list(current_state.triples(
            (None, ProvEntity.iri_generated_at_time, None)))
        most_recent_time = None
        for triple in triples_generated_at_time:
            snapshot_time = triple[2]
            snapshot_date_time = self._convert_to_datetime(snapshot_time)
            if most_recent_time:
                if snapshot_date_time > self._convert_to_datetime(most_recent_time):
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
        """
        Given as input the output of _get_entity_current_state, it populates the graphs 
        relating to the past snapshots of the resource.        
        """
        ordered_data: List[Tuple[str, ConjunctiveGraph]] = sorted(
            entity_current_state[0][self.res].items(),
            key=lambda x: self._convert_to_datetime(x[0]),
            reverse=True
        )
        for index, date_graph in enumerate(ordered_data):
            if index > 0:
                next_snapshot = ordered_data[index-1][0]
                previous_graph: ConjunctiveGraph = copy.deepcopy(
                    entity_current_state[0][self.res][next_snapshot])
                snapshot_uri = list(
                    previous_graph.subjects(object=next_snapshot))[0]
                snapshot_update_query: str = previous_graph.value(
                    subject=snapshot_uri,
                    predicate=ProvEntity.iri_has_update_query,
                    object=None)
                # TODO: To be improved
                if snapshot_update_query is None:
                    entity_current_state[0][self.res][date_graph[0]
                                                   ] = previous_graph
                else:
                    self._manage_update_queries(
                        previous_graph, snapshot_update_query)
                    entity_current_state[0][self.res][date_graph[0]
                                                   ] = previous_graph
        for time in list(entity_current_state[0][self.res]):
            cg_no_pro = entity_current_state[0][self.res].pop(time)
            cg_no_pro.remove((None, ProvEntity.iri_generated_at_time, None))
            cg_no_pro.remove((None, ProvEntity.iri_was_attributed_to, None))
            cg_no_pro.remove((None, ProvEntity.iri_had_primary_source, None))
            cg_no_pro.remove((None, ProvEntity.iri_has_update_query, None))
            cg_no_pro.remove((None, ProvEntity.iri_description, None))
            time_no_tz = self._convert_to_datetime(time)
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
        #     GRAPH ?c {?s ?p ?o}
        #     BIND (<{res}> AS ?s)
        # }
        #
        # Aftwerwards, the rdflib add method can be used to add quads to a Conjunctive Graph,
        # where the fourth element is the context.
        if self.related_entities_history:
            query_dataset = f"""
                SELECT DISTINCT ?s ?p ?o ?c
                WHERE {{
                    GRAPH ?c {{?s ?p ?o}}
                    {{BIND (<{self.res}> AS ?s)}}
                    UNION 
                    {{BIND (<{self.res}> AS ?o)}}
                }}   
            """
        else:
            query_dataset = f"""
                SELECT DISTINCT ?s ?p ?o ?c
                WHERE {{
                    GRAPH ?c {{?s ?p ?o}}
                    BIND (<{self.res}> AS ?s) 
                }}   
            """
        return Sparql(query_dataset).run_construct_query()

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
                OPTIONAL {{
                        ?snapshot <{ProvEntity.iri_had_primary_source}> ?source.
                    }}   
                OPTIONAL {{
                        ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
                    }}  
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
                OPTIONAL {{
                        ?snapshot <{ProvEntity.iri_has_update_query}> ?updateQuery.
                    }}   
                }}
            """
        return Sparql(query_provenance).run_construct_query()

    @classmethod
    def _convert_to_datetime(cls, time_string: str) -> datetime:
        return parser.parse(time_string).replace(tzinfo=None)


def get_entities_histories(res_set: Set[str], include_prov_metadata:bool=False) -> Tuple[Dict[str, Dict[str, ConjunctiveGraph]], Dict]:
    """
    Given a set of entities URIs it returns the history of those entities. 
    You can also specify via the related_entities_history parameter
    if you are also interested in the history of related entities.
    It returns a dictionary according to the following model: ::

        {
            "entity_1_URI": {
                "time_snapshot_1": graph_at_time_1,
                "time_snapshot_2": graph_at_time_2,
                "time_snapshot_n": graph_at_time_n
            },
            "entity_2_URI": {
                "time_snapshot_1": graph_at_time_1,
                "time_snapshot_2": graph_at_time_2,
                "time_snapshot_n": graph_at_time_n
            },
            "entity_n_URI": {
                "time_snapshot_1": graph_at_time_1,
                "time_snapshot_2": graph_at_time_2,
                "time_snapshot_n": graph_at_time_n
            }
        }

    :param res_set: A set of the entities URI you want to retrieve the history about.
    :type res_set: set.
    :param related_entities_history: True if you also want to return information on related entities, that is, the ones that have the URIs in the res_set parameter as object, False otherwise. The default is False. 
    :type related_entities_history: bool.
    :returns:  Dict[str, Dict[str, ConjunctiveGraph]] -- A dictionary containing the graphs related to each considered entities in each of the existing snapshots of these entities.
    """
    entities_histories = [dict(), dict()]
    for res in res_set:
        agnosticEntity = AgnosticEntity(res, related_entities_history=False)
        history_and_metadata = agnosticEntity.get_history(include_prov_metadata=include_prov_metadata)
        if history_and_metadata:
            history = history_and_metadata[0]
            entities_histories[0].update(history)
        if include_prov_metadata:
            metadata = history_and_metadata[1]
            entities_histories[1].update(metadata)
    return tuple(entities_histories)
