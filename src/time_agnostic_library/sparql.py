#!/usr/bin/python
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


import re
import zipfile

from rdflib import Dataset, Graph
from rdflib.term import Literal, Node, URIRef
from sparqlite import SPARQLClient

from time_agnostic_library.prov_entity import ProvEntity

CONFIG_PATH = "./config.json"

_PROV_PROPERTY_STRINGS: tuple[str, ...] = tuple(str(p) for p in ProvEntity.get_prov_properties())
_SELECT_QUERY_RE = re.compile(r'^\s*SELECT\b', re.IGNORECASE)

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
            "graphdb_connector_name": "fts"
        }

    :param config_path: The path to the configuration file.
    :type config_path: str, optional
    """
    def __init__(self, query:str, config:dict):
        self.query = query
        self.config = config
        if any(uri in query for uri in _PROV_PROPERTY_STRINGS):
            self.storer:dict = config["provenance"]
        else:
            self.storer:dict = config["dataset"]

    def run_select_query(self) -> dict:
        """
        Given a SELECT query, it returns the results in SPARQL JSON bindings format.

        :returns:  dict -- A dictionary with 'head' and 'results' keys following SPARQL JSON results format.
        """
        output = {'head': {'vars': []}, 'results': {'bindings': []}}
        if self.storer["file_paths"]:
            output = self._get_results_from_files(output)
        if self.storer["triplestore_urls"]:
            output = self._get_results_from_triplestores(output)
        return output

    def _get_results_from_files(self, output: dict) -> dict:
        storer: list[str] = self.storer["file_paths"]
        for file_path in storer:
            file_cg = Dataset(default_union=True)
            if file_path.endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as z, z.open(z.namelist()[0]) as file:
                        file_cg.parse(file=file, format="json-ld")  # type: ignore[arg-type]
            else:
                file_cg.parse(location=file_path, format="json-ld")
            query_results = file_cg.query(self.query)
            # rdflib types vars as Optional, but it's always set for SELECT queries
            assert query_results.vars is not None
            vars_list = [str(var) for var in query_results.vars]
            output['head']['vars'] = vars_list
            for result in query_results:
                binding = {}
                for var in vars_list:
                    value = result[var]  # type: ignore[index]
                    if value is not None:
                        binding[var] = self._format_result_value(value)
                output['results']['bindings'].append(binding)
        return output

    def _get_results_from_triplestores(self, output: dict) -> dict:
        storer = self.storer["triplestore_urls"]
        for url in storer:
            with SPARQLClient(url) as client:
                results = client.query(self.query)
            if not output['head']['vars']:
                output['head']['vars'] = results['head']['vars']
            output['results']['bindings'].extend(results['results']['bindings'])
        return output

    @staticmethod
    def _format_result_value(value: Node) -> dict:
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

    def run_construct_query(self) -> Dataset:
        """
        Given a CONSTRUCT query, it returns the results in a Dataset.

        :returns:  Dataset -- A Dataset containing the results of the query.
        """
        has_files = bool(self.storer["file_paths"])
        has_triplestores = bool(self.storer["triplestore_urls"])
        if has_files and has_triplestores:
            cg = Dataset(default_union=True)
            for quad in self._get_graph_from_files().quads():
                cg.add(quad)  # type: ignore[arg-type]
            for quad in self._get_graph_from_triplestores().quads():
                cg.add(quad)  # type: ignore[arg-type]
            return cg
        if has_triplestores:
            return self._get_graph_from_triplestores()
        if has_files:
            return self._get_graph_from_files()
        return Dataset(default_union=True)

    def run_ask_query(self) -> bool:
        storer = self.storer["triplestore_urls"]
        for url in storer:
            with SPARQLClient(url) as client:
                return client.ask(self.query)
        return False

    def _get_graph_from_files(self) -> Dataset:
        cg = Dataset(default_union=True)
        storer = self.storer["file_paths"]
        for file_path in storer:
            file_cg = Dataset(default_union=True)
            if file_path.endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as z, z.open(z.namelist()[0]) as file:
                        file_cg.parse(file=file, format="json-ld")  # type: ignore[arg-type]
            else:
                file_cg.parse(location=file_path, format="json-ld")
            results = file_cg.query(self.query)
            for result in results:
                cg.add(result)  # type: ignore[arg-type]
        return cg

    def _get_graph_from_triplestores(self) -> Dataset:
        cg = Dataset(default_union=True)
        storer = self.storer["triplestore_urls"]
        is_select = bool(_SELECT_QUERY_RE.match(self.query))
        for url in storer:
            with SPARQLClient(url) as client:
                if is_select:
                    results = client.query(self.query)
                    for quad in results["results"]["bindings"]:
                        quad_to_add = []
                        for var in results["head"]["vars"]:
                            if quad[var]["type"] == "uri":
                                quad_to_add.append(URIRef(quad[var]["value"]))
                            else:
                                if 'datatype' in quad[var]:
                                    quad_to_add.append(Literal(quad[var]["value"], datatype=quad[var]['datatype']))
                                elif 'xml:lang' in quad[var]:
                                    quad_to_add.append(Literal(quad[var]["value"], lang=quad[var]['xml:lang']))
                                else:
                                    quad_to_add.append(Literal(quad[var]["value"]))
                        cg.add(tuple(quad_to_add))  # type: ignore[arg-type]
                else:
                    raw_result = client.construct(self.query)
                    result_graph = Graph()
                    result_graph.parse(data=raw_result, format="nt")
                    for s, p, o in result_graph.triples((None, None, None)):
                        cg.add((s, p, o))
        return cg

    @classmethod
    def _get_tuples_set(cls, result_dict:dict, output:set, vars_list: list) -> None:
        results_list = []
        for var in vars_list:
            if str(var) in result_dict:
                val = result_dict[str(var)]
                if isinstance(val, dict) and "value" in val:
                    results_list.append(str(val["value"]))
                else:
                    results_list.append(str(val))
            else:
                results_list.append(None)
        output.add(tuple(results_list))
