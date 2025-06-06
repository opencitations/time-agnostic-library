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


from typing import Set, Tuple, List
import zipfile
from rdflib import XSD, ConjunctiveGraph
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib.plugins.sparql.processor import prepareQuery
from rdflib.plugins.sparql.sparql import Query
from rdflib.term import Literal, URIRef, _toPythonMapping
from SPARQLWrapper import JSON, POST, RDFXML, SPARQLWrapper

from time_agnostic_library.prov_entity import ProvEntity

CONFIG_PATH = "./config.json"

class Sparql:
    """
    The Sparql class handles SPARQL queries.
    It is instantiated by passing as a parameter 
    the path to a configuration file, whose default location is "./config.json".
    The configuration file must be in JSON format and contain information on the sources to be queried. 
    There are two types of sources: dataset and provenance sources and they need to be specified separately.
    Both triplestores and JSON files are supported. 
    In addition, some optional values can be set to make executions faster and more efficient.

    - **blazegraph_full_text_search**: Specify an affirmative Boolean value if Blazegraph was used as a triplestore, and a textual index was built to speed up queries. For more information, see https://github.com/blazegraph/database/wiki/Rebuild_Text_Index_Procedure. The allowed values are "true", "1", 1, "t", "y", "yes", "ok", or "false", "0", 0, "n", "f", "no".
    - **graphdb_connector_name**: Specify the name of the Lucene connector if GraphDB was used as a triplestore and a textual index was built to speed up queries. For more information, see https://graphdb.ontotext.com/documentation/free/general-full-text-search-with-connectors.html.
    - **cache_triplestore_url**: Specifies the triplestore URL to use as a cache to make queries faster. If your triplestore uses different endpoints for reading and writing (e.g. GraphDB), specify the endpoint for reading in the "endpoint" field and the endpoint for writing in the "update_endpoint" field. If there is only one endpoint (e.g. Blazegraph), specify it in both fields.
    
    Here is an example of the configuration file content: ::

        {
            "dataset": {
                "triplestore_urls": ["http://127.0.0.1:7200/repositories/data"],
                "file_paths": []
            },
            "provenance": {
                "triplestore_urls": [],
                "file_paths": ["./prov.json"]
            },
            "blazegraph_full_text_search": "no",
            "graphdb_connector_name": "fts",
            "cache_triplestore_url": {
                "endpoint": "http://127.0.0.1:7200/repositories/cache",
                "update_endpoint": "http://127.0.0.1:7200/repositories/cache/statements"
            }
        }            

    :param config_path: The path to the configuration file.
    :type config_path: str, optional
    """
    def __init__(self, query:str, config:dict):
        self.query = query
        self.config = config
        prov_properties = ProvEntity.get_prov_properties()
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

        :returns:  Set[Tuple] -- A set of tuples, in which the positional value of the elements in the tuples is equivalent to the variables indicated in the query.
        """
        output = {'head': {'vars': []}, 'results': {'bindings': []}}
        if self.storer["file_paths"]:
            output = self._get_results_from_files(output)
        if self.storer["triplestore_urls"]:
            output = self._get_results_from_triplestores(output)
        return output

    def _get_results_from_files(self, output: dict) -> dict:
        storer: List[str] = self.storer["file_paths"]
        for file_path in storer:
            file_cg = ConjunctiveGraph()
            if file_path.endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as z:
                    with z.open(z.namelist()[0]) as file:
                        file_cg.parse(file=file, format="json-ld")
            else:
                file_cg.parse(location=file_path, format="json-ld")
            query_results = file_cg.query(self.query)
            vars_list = [str(var) for var in query_results.vars]
            output['head']['vars'] = vars_list
            for result in query_results:
                binding = {}
                for var in vars_list:
                    value = result.get(var)
                    if value is not None:
                        binding[var] = self._format_result_value(value)
                output['results']['bindings'].append(binding)
        return output
    
    def _get_results_from_triplestores(self, output: dict) -> dict:
        storer = self.storer["triplestore_urls"]
        for url in storer:
            sparql = SPARQLWrapper(url)
            sparql.setMethod(POST)
            sparql.setQuery(self.query)
            sparql.setReturnFormat(JSON)
            results = sparql.queryAndConvert()
            if not output['head']['vars']:
                output['head']['vars'] = results['head']['vars']
            output['results']['bindings'].extend(results['results']['bindings'])
        return output

    def _format_result_value(self, value):
        if isinstance(value, URIRef):
            return {'type': 'uri', 'value': str(value)}
        elif isinstance(value, Literal):
            result = {'type': 'literal', 'value': str(value)}
            if value.datatype:
                result['datatype'] = str(value.datatype)
            if value.language:
                result['xml:lang'] = value.language
            return result
        else:
            return {'type': 'literal', 'value': str(value)}

    def run_construct_query(self) -> ConjunctiveGraph:
        """
        Given a CONSTRUCT query, it returns the results in a ConjunctiveGraph. 

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

    def run_ask_query(self) -> bool:
        storer = self.storer["triplestore_urls"]
        for url in storer:
            sparql = SPARQLWrapper(url)
            sparql.setMethod(POST)
            sparql.setQuery(self.query)
            sparql.setReturnFormat(JSON)
            results = sparql.queryAndConvert()
            return results.get('boolean', False)
        return False

    def _get_graph_from_files(self) -> ConjunctiveGraph:
        cg = ConjunctiveGraph()
        storer = self.storer["file_paths"]
        for file_path in storer:
            file_cg = ConjunctiveGraph()
            if file_path.endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as z:
                    with z.open(z.namelist()[0]) as file:
                        file_cg.parse(file=file, format="json-ld")
            else:
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
            sparql.setOnlyConneg(True)
            # A SELECT hack can be used to return RDF quads in named graphs, 
            # since the CONSTRUCT allows only to return triples in SPARQL 1.1.
            # Here is an exemple of SELECT hack
            #
            # SELECT DISTINCT ?s ?p ?o ?c
            # WHERE {{
            #     BIND (<{self.res}> AS ?s)
            #     GRAPH ?c {{?s ?p ?o}}
            # }}
            #
            # Aftwerwards, the rdflib add method can be used to add quads to a Conjunctive Graph, 
            # where the fourth element is the context.    
            if algebra.name == "SelectQuery":
                sparql.setReturnFormat(JSON)
                sparql.setOnlyConneg(True)
                results = sparql.queryAndConvert()
                for quad in results["results"]["bindings"]:
                    quad_to_add = list()
                    for var in results["head"]["vars"]:
                        if quad[var]["type"] == "uri":
                            quad_to_add.append(URIRef(quad[var]["value"]))
                        else:
                            if 'datatype' in quad[var]:
                                quad_to_add.append(Literal(quad[var]["value"], datatype=quad[var]['datatype']))
                            else:
                                quad_to_add.append(Literal(quad[var]["value"], datatype=XSD.string))
                    cg.add(tuple(quad_to_add))
            elif algebra.name == "ConstructQuery":
                sparql.setReturnFormat(RDFXML)
                sparql.setOnlyConneg(True)
                cg += sparql.queryAndConvert()     
        return cg        
    
    @classmethod
    def _get_tuples_set(cls, result_dict:dict, output:set, vars_list: list):
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