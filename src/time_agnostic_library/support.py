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

from dateutil import parser
from rdflib import Dataset, Literal

CONFIG_PATH = './config.json'


def generate_config_file(
        config_path:str=CONFIG_PATH, dataset_urls:list | None=None, dataset_dirs:list | None=None, dataset_is_quadstore:bool=True,
        provenance_urls:list | None=None, provenance_dirs:list | None=None, provenance_is_quadstore:bool=True,
        blazegraph_full_text_search:bool=False, fuseki_full_text_search:bool=False, virtuoso_full_text_search:bool=False,
        graphdb_connector_name:str='', cache_endpoint:str='', cache_update_endpoint:str='') -> dict:
    '''
    Given the configuration parameters, a file compliant with the syntax of the time-agnostic-library configuration files is generated.
    :param config_path: The output configuration file path
    :type config_path: str
    :param dataset_urls: A list of triplestore URLs containing data
    :type dataset_urls: list
    :param dataset_dirs: A list of directories containing data
    :type dataset_dirs: list
    :param dataset_is_quadstore: Indicates if the dataset store is a quadstore
    :type dataset_is_quadstore: bool
    :param provenance_urls: A list of triplestore URLs containing provenance metadata
    :type provenance_urls: list
    :param provenance_dirs: A list of directories containing provenance metadata
    :type provenance_dirs: list
    :param provenance_is_quadstore: Indicates if the provenance store is a quadstore
    :type provenance_is_quadstore: bool
    :param blazegraph_full_text_search: True if Blazegraph was used as a triplestore, and a textual index was built to speed up queries
    :type blazegraph_full_text_search: bool
    :param fuseki_full_text_search: True if Fuseki was used as a triplestore, and a textual index was built to speed up queries
    :type fuseki_full_text_search: bool
    :param virtuoso_full_text_search: True if Virtuoso was used as a triplestore, and a textual index was built to speed up queries
    :type virtuoso_full_text_search: bool
    :param graphdb_connector_name: The name of the Lucene connector if GraphDB was used as a triplestore and a textual index was built
    :type graphdb_connector_name: str
    :param cache_endpoint: A triplestore URL to use as a cache to make queries on provenance faster
    :type cache_endpoint: str
    :param cache_update_endpoint: If your triplestore uses different endpoints for reading and writing (e.g. GraphDB), specify the endpoint for writing
    :type cache_update_endpoint: str
    '''
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
        'cache_triplestore_url': {
            'endpoint': cache_endpoint,
            'update_endpoint': cache_update_endpoint
        }
    }
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f)
    return config

def convert_to_datetime(time_string: str | None, stringify: bool = False) -> datetime | str | None:
    if time_string and time_string != 'None':
        time = parser.parse(time_string)
        if time.tzinfo is None:
            time = time.replace(tzinfo=timezone.utc)
        else:
            time = time.astimezone(timezone.utc)

        if stringify:
            return time.isoformat()
        return time
    return None

def _to_nt_sorted_list(cg:Dataset) -> list | None:
    if cg is None:
        return None
    normalized_cg = Dataset(default_union=True)
    for quad in cg.quads():
        normalized_quad = tuple(Literal(str(el), datatype=None) if isinstance(el, Literal) else el for el in quad)
        normalized_cg.add(normalized_quad)  # type: ignore[arg-type]
    nt_list = re.split(r'\s?\.?\n+', normalized_cg.serialize(format='nt'))
    nt_list = filter(None, nt_list)
    sorted_nt_list = sorted(nt_list)
    return sorted_nt_list

def _to_dict_of_nt_sorted_lists(dictionary: dict) -> dict:
    dict_of_nt_sorted_lists = {}
    for key, value in dictionary.items():
        if isinstance(value, Dataset):
            dict_of_nt_sorted_lists[key] = _to_nt_sorted_list(value)
        else:
            for snapshot, cg in value.items():
                dict_of_nt_sorted_lists.setdefault(key, {})
                dict_of_nt_sorted_lists[key][snapshot] = _to_nt_sorted_list(cg)
    return dict_of_nt_sorted_lists

def _to_dataset(nt_list:list[str]) -> Dataset:
    cg = Dataset(default_union=True)
    for triple in nt_list:
        cg.parse(data=triple + '.', format='nt')
    return cg

def _to_dict_of_datasets(dictionary: dict) -> dict:
    dict_of_datasets = {}
    for key, value in dictionary.items():
        if isinstance(value, list):
            cg = _to_dataset(value)
            dict_of_datasets.setdefault(key, {})
            dict_of_datasets[key] = cg
        else:
            for snapshot, triples in value.items():
                cg = _to_dataset(triples)
                dict_of_datasets.setdefault(key, {})
                dict_of_datasets[key][snapshot] = cg
    return dict_of_datasets
