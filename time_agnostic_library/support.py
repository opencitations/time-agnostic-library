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

from typing import Dict, List

import json
from rdflib.graph import ConjunctiveGraph
from SPARQLWrapper import SPARQLWrapper, JSON, POST

CONFIG_PATH = "./config.json"

def empty_the_cache(config_path:str = CONFIG_PATH):
    with open(config_path, encoding="utf8") as json_file:
        cache_triplestore_url = json.load(json_file)["cache_triplestore_url"]
    if cache_triplestore_url:
        timestamps = []
        query_timestamps = """
            SELECT DISTINCT ?g 
            WHERE {GRAPH ?g {?s ?p ?o}}
        """
        sparql = SPARQLWrapper(cache_triplestore_url)
        sparql.setQuery(query_timestamps)
        sparql.setReturnFormat(JSON)
        results = sparql.queryAndConvert()
        for result in results["results"]["bindings"]:
            timestamps.append(result["g"]["value"])
        for timestamp in timestamps:
            clear = f"CLEAR GRAPH <{timestamp}>"
            sparql.setQuery(clear)
            sparql.setMethod(POST)
            sparql.query()
    
def _to_nt_sorted_list(cg:ConjunctiveGraph) -> list:
    if cg is None:
        return None
    nt_list = str(cg.serialize(format="nt")).split(r".\n")
    sorted_nt_list = sorted([triple.replace("b\'", "").strip() for triple in nt_list if triple != r"b'\n'" and triple != r"\n'"])
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
        cg.parse(data=triple + ".", format="nt")
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










