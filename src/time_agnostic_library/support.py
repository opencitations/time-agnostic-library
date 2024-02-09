#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.


import json
import re
from datetime import datetime
from typing import Dict, List, Tuple

from dateutil import parser
from rdflib import Literal
from rdflib.graph import ConjunctiveGraph
from SPARQLWrapper import POST, SPARQLWrapper

CONFIG_PATH = './config.json'

def empty_the_cache(config_path:str = CONFIG_PATH) -> None:
    '''
    It empties the entire cache.
    :param config_path: The path to the configuration file.
    :type config_path: str, optional
    '''
    with open(config_path, encoding='utf8') as json_file:
        cache_triplestores = json.load(json_file)['cache_triplestore_url']
        read_triplestore_url = cache_triplestores['endpoint']
        write_triplestore_url = cache_triplestores['update_endpoint']
    if write_triplestore_url:
        delete_everything_query = '''
            DELETE {GRAPH ?g {?s ?p ?o}}
            WHERE {GRAPH ?g {?s ?p ?o}}
        '''
        sparql = SPARQLWrapper(read_triplestore_url, updateEndpoint=write_triplestore_url)
        sparql.setQuery(delete_everything_query)
        sparql.setMethod(POST)
        sparql.query()

def generate_config_file(
        config_path:str=CONFIG_PATH, dataset_urls:list=list(), dataset_dirs:list=list(), provenance_urls:list=list(), provenance_dirs:list=list(), 
        blazegraph_full_text_search:bool=False, fuseki_full_text_search:bool=False, virtuoso_full_text_search:bool=False, graphdb_connector_name:str='', 
        cache_endpoint:str='', cache_update_endpoint:str='') -> None:
    '''
    Given the configuration parameters, a file compliant with the syntax of the time-agnostic-library configuration files is generated.
    :param config_path: The output configuration file path
    :type config_path: str
    :param dataset_urls: A list of triplestore URLs containing data
    :type dataset_urls: list
    :param dataset_dirs: A list of directories containing data
    :type dataset_dirs: list
    :param provenance_urls: A list of triplestore URLs containing provenance metadata
    :type provenance_urls: list
    :param provenance_dirs: A list of directories containing provenance metadata
    :type provenance_dirs: list
    :param blazegraph_full_text_search: True if Blazegraph was used as a triplestore, and a textual index was built to speed up queries. For more information, see https://github.com/blazegraph/database/wiki/Rebuild_Text_Index_Procedure
    :type blazegraph_full_text_search: bool
    :param fuseki_full_text_search: True if Fuseki was used as a triplestore, and a textual index was built to speed up queries. For more information, see https://jena.apache.org/documentation/query/text-query.html
    :type fuseki_full_text_search: bool
    :param virtuoso_full_text_search: True if Virtuoso was used as a triplestore, and a textual index was built to speed up queries. For more information, see https://docs.openlinksw.com/virtuoso/rdfsparqlrulefulltext/
    :type virtuoso_full_text_search: bool
    :param graphdb_connector_name: The name of the Lucene connector if GraphDB was used as a triplestore and a textual index was built to speed up queries. For more information, see [https://graphdb.ontotext.com/documentation/free/general-full-text-search-with-connectors.html](https://graphdb.ontotext.com/documentation/free/general-full-text-search-with-connectors.html)
    :type graphdb_connector_name: str
    :param cache_endpoint: A triplestore URL to use as a cache to make queries on provenance faster
    :type cache_endpoint: str
    :param cache_update_endpoint: If your triplestore uses different endpoints for reading and writing (e.g. GraphDB), specify the endpoint for writing in this parameter
    :type cache_update_endpoint: str
    '''
    config = {
        'dataset': {
            'triplestore_urls': dataset_urls,
            'file_paths': dataset_dirs
        },
        'provenance': {
            'triplestore_urls': provenance_urls,
            'file_paths': provenance_dirs
        },
        'blazegraph_full_text_search': blazegraph_full_text_search,
        'fuseki_full_text_search': fuseki_full_text_search,
        'virtuoso_full_text_search': virtuoso_full_text_search,
        'graphdb_connector_name': graphdb_connector_name,
        'cache_triplestore_url': {
            'endpoint': cache_endpoint,
            'update_endpoint': cache_update_endpoint
        }    
    }
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f)

def convert_to_datetime(time_string:str, stringify:bool=False) -> datetime:
    if time_string and time_string != 'None':
        time = parser.parse(time_string).replace(tzinfo=None)
        if stringify:
            time = time.isoformat()
        return time

def is_within_time_range(range_to_eval:Tuple[str, str], given_range:Tuple[str, str]) -> bool:
    if not given_range:
        return True
    range_to_eval_start = convert_to_datetime(range_to_eval[0])
    range_to_eval_end = convert_to_datetime(range_to_eval[1])
    given_range_start = convert_to_datetime(given_range[0])
    given_range_end = convert_to_datetime(given_range[1])
    is_within_time_range = False
    if given_range_start:
        if range_to_eval_start >= given_range_start:
            if given_range_end:
                if range_to_eval_end <= given_range_end:
                    is_within_time_range = True
            else:
                is_within_time_range = True
    elif given_range_end:
        if range_to_eval_end <= given_range_end:
            is_within_time_range = True
    return is_within_time_range

def _to_nt_sorted_list(cg:ConjunctiveGraph) -> list:
    if cg is None:
        return None
    normalized_cg = ConjunctiveGraph()
    for quad in cg.quads():
        normalized_quad = tuple(Literal(str(el), datatype=None) if isinstance(el, Literal) else el for el in quad)
        normalized_cg.add(normalized_quad)
    nt_list = re.split('\s?\.?\n+', normalized_cg.serialize(format='nt'))
    nt_list = filter(None, nt_list)
    sorted_nt_list = sorted(nt_list)
    return sorted_nt_list

def _to_dict_of_nt_sorted_lists(dictionary:Dict[str, Dict[str, ConjunctiveGraph]]) -> Dict[str, Dict[str, List[str]]]:
    dict_of_nt_sorted_lists = dict()
    for key, value in dictionary.items():
        if isinstance(value, ConjunctiveGraph):
            dict_of_nt_sorted_lists[key] = _to_nt_sorted_list(value)
        else:
            for snapshot, cg in value.items():
                dict_of_nt_sorted_lists.setdefault(key, dict())
                dict_of_nt_sorted_lists[key][snapshot] = _to_nt_sorted_list(cg)
    return dict_of_nt_sorted_lists

def _to_conjunctive_graph(nt_list:List[str]) -> ConjunctiveGraph:
    cg = ConjunctiveGraph()
    for triple in nt_list:
        cg.parse(data=triple + '.', format='nt')
    return cg

def _to_dict_of_conjunctive_graphs(dictionary:Dict[str, Dict[str, List]]) -> Dict[str, Dict[str, ConjunctiveGraph]]:
    dict_of_conjunctive_graphs = dict()
    for key, value in dictionary.items():
        if isinstance(value, list):
            cg = _to_conjunctive_graph(value)
            dict_of_conjunctive_graphs.setdefault(key, dict())
            dict_of_conjunctive_graphs[key] = cg
        else:
            for snapshot, triples in value.items():
                cg = _to_conjunctive_graph(triples)
                dict_of_conjunctive_graphs.setdefault(key, dict())
                dict_of_conjunctive_graphs[key][snapshot] = cg
    return dict_of_conjunctive_graphs