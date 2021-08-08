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

from typing import Set, Tuple

import json
from SPARQLWrapper import SPARQLWrapper, POST, RDFXML, JSON
from rdflib import ConjunctiveGraph, XSD
from rdflib.term import _toPythonMapping
from rdflib.term import URIRef, Literal
from rdflib.plugins.sparql.processor import prepareQuery
from rdflib.plugins.sparql.sparql import Query
from rdflib.plugins.sparql.parserutils import CompValue

from time_agnostic_library.prov_entity import ProvEntity

CONFIG_PATH = "./config.json"

class Sparql:
    """
    The Sparql class handles SPARQL queries.
    It is instantiated by passing as a parameter 
    the path to a configuration file, whose default location is "./config.json".
    The configuration file must be in JSON format and contain information on the sources to be queried. 
    There are two types of sources, dataset sources and provenance sources, and they need to be specified separately. 
    Each must contain the URLs of the triplestore on which to search for information and/or the paths of the files that 
    contain the information. The files must be in JSON format. Here is an example of the configuration file content: ::

        {
            "dataset": {
                "triplestore_urls": ["http://localhost:9999/blazegraph/sparql"],
                "file_paths": []
            },
            "provenance": {
                "triplestore_urls": [],
                "file_paths": ["./test/scientometrics_prov.json"]
            }
        }            

    :param config_path: The path to the configuration file.
    :type config_path: str.
    """
    def __init__(self, query:str, config_path:str=CONFIG_PATH):
        self.query = query
        config_path = config_path
        prov_properties = ProvEntity.get_prov_properties()
        with open(config_path, encoding="utf8") as json_file:
            config:list = json.load(json_file)
        if any(uri in query for uri in prov_properties):
            self.storer:dict = config["provenance"]
        else:
            self.storer:dict = config["dataset"]
        self._hack_dates()

    @classmethod
    def _hack_dates(cls) -> None:
        if XSD.gYear in _toPythonMapping:
            _toPythonMapping.pop(XSD.gYear)
        if XSD.gYearMonth in _toPythonMapping:
            _toPythonMapping.pop(XSD.gYearMonth)
    
    def run_select_query(self) -> Set[Tuple]:
        """
        Given a SELECT query, it returns the results in a set of tuples. 

        :param query: A SELECT query.
        :type query: str.
        :returns:  Set[Tuple] -- A set of tuples, in which the positional value of the tuples corresponds to the positional value of the variables indicated in the query.
        """
        output = set()
        if self.storer["file_paths"]:
            output.update(self._get_tuples_from_files())
        if self.storer["triplestore_urls"]:
            output.update(self._get_tuples_from_triplestores())
        output = set(self._cut_by_limit(list(output)))
        return output

    def _get_tuples_from_files(self) -> Set[Tuple]:
        output = set()
        storer = self.storer["file_paths"]
        for file_path in storer:
            file_cg = ConjunctiveGraph()
            file_cg.parse(location=file_path, format="json-ld")
            results = file_cg.query(self.query)._get_bindings()
            for result in results:
                self._get_tuples_set(self.query, result, output)
        return output
    
    def _get_tuples_from_triplestores(self) -> Set[Tuple]:
        output = set()
        storer = self.storer["triplestore_urls"]
        for url in storer:
            sparql = SPARQLWrapper(url)
            sparql.setMethod(POST)
            sparql.setQuery(self.query)
            sparql.setReturnFormat(JSON)
            results = sparql.queryAndConvert()
            for result_dict in results["results"]["bindings"]:
                self._get_tuples_set(self.query, result_dict, output)
        return output
            
    def run_construct_query(self) -> ConjunctiveGraph:
        """
        Given a CONSTRUCT query, it returns the results in a ConjunctiveGraph. 

        :param query: A CONSTRUCT query.
        :type query: str.
        :returns:  ConjunctiveGraph -- A ConjunctiveGraph containing the results of the query. 
        """
        cg = ConjunctiveGraph()
        if self.storer["file_paths"]:
            results = self._get_graph_from_files()
            for quad in results.quads():
                cg.add(quad)
        if self.storer["triplestore_urls"]:
            results = self._get_graph_from_triplestores()
            for quad in results.quads():
                cg.add(quad)
        cg = self._cut_by_limit(cg)
        return cg
    
    def _get_graph_from_files(self) -> ConjunctiveGraph:
        cg = ConjunctiveGraph()
        storer = self.storer["file_paths"]
        for file_path in storer:
            file_cg = ConjunctiveGraph()
            file_cg.parse(location=file_path, format="json-ld")
            results = file_cg.query(self.query)
            for result in results:
                cg.add(result)
        return cg

    def _get_graph_from_triplestores(self) -> ConjunctiveGraph:
        cg = ConjunctiveGraph()
        storer = self.storer["triplestore_urls"]
        prepare_query:Query = prepareQuery(self.query)
        algebra:CompValue = prepare_query.algebra
        for url in storer:
            sparql = SPARQLWrapper(url)
            sparql.setMethod(POST)
            sparql.setQuery(self.query)
            # A SELECT hack can be used to return RDF quads in named graphs, 
            # since the CONSTRUCT allows only to return triples in SPARQL 1.1.
            # Here is an exemple of SELECT hack
            #
            # SELECT DISTINCT ?s ?p ?o ?c
            # WHERE {
            #     GRAPH ?c {?s ?p ?o}
            #     BIND (<{self.res}> AS ?s)
            # }}
            #
            # Aftwerwards, the rdflib add method can be used to add quads to a Conjunctive Graph, 
            # where the fourth element is the context.    
            if algebra.name == "SelectQuery":
                sparql.setReturnFormat(JSON)
                results = sparql.queryAndConvert()
                for quad in results["results"]["bindings"]:
                    quad_to_add = list()
                    for var in results["head"]["vars"]:
                        if quad[var]["type"] == "uri":
                            quad_to_add.append(URIRef(quad[var]["value"]))
                        elif quad[var]["type"] == "literal":
                            quad_to_add.append(Literal(quad[var]["value"]))
                    cg.add(tuple(quad_to_add))
            elif algebra.name == "ConstructQuery":
                sparql.setReturnFormat(RDFXML)
                cg += sparql.queryAndConvert()            
        return cg        
    
    @classmethod
    def _get_tuples_set(cls, query:str, result_dict:dict, output:set):
        vars_list = prepareQuery(query).algebra["PV"]
        results_list = list()
        for var in vars_list:
            if str(var) in result_dict:
                results_list.append(str(result_dict[str(var)]["value"] if "value" in result_dict[str(var)] else result_dict[str(var)]))
            else:
                results_list.append(None)
        output.add(tuple(results_list))
    
    def _cut_by_limit(self, input):
        algebra:CompValue = prepareQuery(self.query).algebra
        if "length" in algebra["p"]:
            limit = int(algebra["p"]["length"])
            input = input[:limit]
        return input
    
