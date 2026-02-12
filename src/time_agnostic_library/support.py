#!/usr/bin/python
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
from datetime import datetime, timezone
from functools import lru_cache

from dateutil import parser

CONFIG_PATH = './config.json'

_NT_TERM_RE = re.compile(
    r'<([^>]+)>'
    r'|"((?:[^"\\]|\\.)*)"\^\^<([^>]+)>'
    r'|"((?:[^"\\]|\\.)*)"@([a-zA-Z][\w-]*)'
    r'|"((?:[^"\\]|\\.)*)"'
    r'|(_:\S+)',
    re.DOTALL,
)


def _nt_match_to_n3(match: re.Match) -> str:
    if match.group(1) is not None:
        return f"<{match.group(1)}>"
    if match.group(2) is not None:
        return f'"{match.group(2)}"^^<{match.group(3)}>'
    if match.group(4) is not None:
        return f'"{match.group(4)}"@{match.group(5)}'
    if match.group(6) is not None:
        return f'"{match.group(6)}"'
    return match.group(7)


def generate_config_file(
        config_path:str=CONFIG_PATH, dataset_urls:list | None=None, dataset_dirs:list | None=None, dataset_is_quadstore:bool=True,
        provenance_urls:list | None=None, provenance_dirs:list | None=None, provenance_is_quadstore:bool=True,
        blazegraph_full_text_search:bool=False, fuseki_full_text_search:bool=False, virtuoso_full_text_search:bool=False,
        graphdb_connector_name:str='') -> dict:
    if provenance_dirs is None:
        provenance_dirs = []
    if provenance_urls is None:
        provenance_urls = []
    if dataset_dirs is None:
        dataset_dirs = []
    if dataset_urls is None:
        dataset_urls = []
    config = {
        'dataset': {
            'triplestore_urls': dataset_urls,
            'file_paths': dataset_dirs,
            'is_quadstore': dataset_is_quadstore
        },
        'provenance': {
            'triplestore_urls': provenance_urls,
            'file_paths': provenance_dirs,
            'is_quadstore': provenance_is_quadstore
        },
        'blazegraph_full_text_search': str(blazegraph_full_text_search).lower(),
        'fuseki_full_text_search': str(fuseki_full_text_search).lower(),
        'virtuoso_full_text_search': str(virtuoso_full_text_search).lower(),
        'graphdb_connector_name': graphdb_connector_name,
    }
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f)
    return config

@lru_cache(maxsize=4096)
def _cached_parse(time_string: str) -> datetime:
    time = parser.parse(time_string)
    if time.tzinfo is None:
        return time.replace(tzinfo=timezone.utc)
    return time.astimezone(timezone.utc)

def convert_to_datetime(time_string: str | None, stringify: bool = False) -> datetime | str | None:
    if time_string and time_string != 'None':
        time = _cached_parse(time_string)
        if stringify:
            return time.isoformat()
        return time
    return None

def _strip_literal_datatype(n3: str) -> str:
    if not n3.startswith('"'):
        return n3
    i = 1
    while i < len(n3):
        if n3[i] == '\\':
            i += 2
            continue
        if n3[i] == '"':
            rest = n3[i + 1:]
            if rest.startswith('@'):
                return n3
            return n3[:i + 1]
        i += 1
    return n3

def _to_nt_sorted_list(quads) -> list | None:
    if quads is None:
        return None
    lines = set()
    for q in quads:
        parts = [_strip_literal_datatype(el) for el in q[:3]]
        lines.add(' '.join(parts))
    return sorted(lines)

def _to_dict_of_nt_sorted_lists(dictionary: dict) -> dict:
    result = {}
    for key, value in dictionary.items():
        if isinstance(value, set):
            result[key] = _to_nt_sorted_list(value)
        else:
            result.setdefault(key, {})
            for snapshot, quad_set in value.items():
                result[key][snapshot] = _to_nt_sorted_list(quad_set)
    return result

def _nt_list_to_quad_set(nt_list: list[str]) -> set[tuple[str, ...]]:
    result = set()
    for line in nt_list:
        if not line.strip():
            continue
        matches = list(_NT_TERM_RE.finditer(line))
        if len(matches) >= 3:
            result.add(tuple(_nt_match_to_n3(m) for m in matches[:3]))
    return result

def _to_dict_of_quad_sets(dictionary: dict) -> dict:
    result = {}
    for key, value in dictionary.items():
        if isinstance(value, list):
            result[key] = _nt_list_to_quad_set(value)
        else:
            result.setdefault(key, {})
            for snapshot, triples in value.items():
                result[key][snapshot] = _nt_list_to_quad_set(triples)
    return result
