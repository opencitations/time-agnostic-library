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


import zipfile

from rdflib import Dataset
from rdflib.term import Literal, URIRef
from sparqlite import SPARQLClient

from time_agnostic_library.prov_entity import ProvEntity

__all__ = [
    "Sparql",
    "_binding_to_n3",
    "_n3_value",
    "_n3_to_binding",
]

CONFIG_PATH = "./config.json"

_PROV_PROPERTY_STRINGS: tuple[str, ...] = tuple(ProvEntity.get_prov_properties())


def _escape_n3(v: str) -> str:
    return v.replace('\\', '\\\\').replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')


def _binding_to_n3(val: dict) -> str:
    if val['type'] == 'uri':
        return f"<{val['value']}>"
    if val['type'] == 'bnode':
        return f"_:{val['value']}"
    escaped = _escape_n3(val['value'])
    if 'datatype' in val:
        return f'"{escaped}"^^<{val["datatype"]}>'
    if 'xml:lang' in val:
        return f'"{escaped}"@{val["xml:lang"]}'
    return f'"{escaped}"'


def _find_closing_quote(n3: str) -> int:
    pos = n3.find('"', 1)
    while pos > 0:
        num_backslashes = 0
        check = pos - 1
        while check >= 1 and n3[check] == '\\':
            num_backslashes += 1
            check -= 1
        if num_backslashes % 2 == 0:
            return pos
        pos = n3.find('"', pos + 1)
    return -1


def _unescape_n3(raw: str) -> str:
    out: list[str] = []
    i = 0
    while i < len(raw):
        if raw[i] == '\\' and i + 1 < len(raw):
            nxt = raw[i + 1]
            if nxt == 'n':
                out.append('\n')
            elif nxt == 'r':
                out.append('\r')
            elif nxt == '"':
                out.append('"')
            elif nxt == '\\':
                out.append('\\')
            else:
                out.append(raw[i])
                out.append(nxt)
            i += 2
        else:
            out.append(raw[i])
            i += 1
    return ''.join(out)


def _parse_n3_literal(n3: str) -> tuple[str, str]:
    quote_end = _find_closing_quote(n3)
    if quote_end == -1:
        return n3, ''
    raw = n3[1:quote_end]
    return _unescape_n3(raw), n3[quote_end + 1:]


def _n3_value(n3: str) -> str:
    if n3.startswith('<') and n3.endswith('>'):
        return n3[1:-1]
    if n3.startswith('_:'):
        return n3[2:]
    value, _ = _parse_n3_literal(n3)
    return value


def _n3_to_binding(n3: str) -> dict:
    if n3.startswith('<') and n3.endswith('>'):
        return {'type': 'uri', 'value': n3[1:-1]}
    if n3.startswith('_:'):
        return {'type': 'bnode', 'value': n3[2:]}
    value, rest = _parse_n3_literal(n3)
    if rest.startswith('^^<') and rest.endswith('>'):
        return {'type': 'literal', 'value': value, 'datatype': rest[3:-1]}
    if rest.startswith('@'):
        return {'type': 'literal', 'value': value, 'xml:lang': rest[1:]}
    return {'type': 'literal', 'value': value}


class Sparql:
    def __init__(self, query:str, config:dict):
        self.query = query
        self.config = config
        if any(uri in query for uri in _PROV_PROPERTY_STRINGS):
            self.storer:dict = config["provenance"]
        else:
            self.storer:dict = config["dataset"]

    def run_select_query(self) -> dict:
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
    def _format_result_value(value) -> dict:
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

    def run_select_to_quad_set(self) -> set[tuple[str, ...]]:
        results = self.run_select_query()
        output: set[tuple[str, ...]] = set()
        vars_list = results['head']['vars']
        for binding in results['results']['bindings']:
            components: list[str] = []
            skip = False
            for var in vars_list:
                if var not in binding:
                    skip = True
                    break
                components.append(_binding_to_n3(binding[var]))
            if not skip:
                output.add(tuple(components))
        return output

    def run_ask_query(self) -> bool:
        storer = self.storer["triplestore_urls"]
        for url in storer:
            with SPARQLClient(url) as client:
                return client.ask(self.query)
        return False

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
