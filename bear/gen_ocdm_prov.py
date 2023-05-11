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

from __future__ import annotations

import gzip
import json
import os
from typing import Dict

from rdflib import Literal, URIRef
from rdflib_ocdm.counter_handler.counter_handler import CounterHandler
from rdflib_ocdm.counter_handler.sqlite_counter_handler import \
    SqliteCounterHandler
from rdflib_ocdm.ocdm_graph import OCDMGraph
from rdflib_ocdm.reader import Reader
from rdflib_ocdm.storer import Storer
from tqdm import tqdm


def _reader_generator(reader):
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024 * 1024)

def raw_newline_count_gzip(fname):
    f = gzip.open(fname, 'rb')
    f_gen = _reader_generator(f.read)
    return sum(buf.count(b'\n') for buf in f_gen)

def update_cache(cache_filepath: str, data: dict, processed_files: list) -> None:
    data['processed_files'].extend(processed_files)
    with open(cache_filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f)

def read_cache(cache_filepath: str) -> dict:
    if os.path.exists(cache_filepath):
        with open(cache_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = {'processed_files': []}
    return data

def generate_ocdm_provenance(cb_dir: str, ts_url: str, counter_handler: CounterHandler, cache_filepath: str, base_dir: str = None):
    ocdm_graph = OCDMGraph(counter_handler)
    versions = set()
    for filename in os.listdir(cb_dir):
        versions.add(filename.split('_')[1].replace('.nt.gz', ''))
    versions = sorted(list(versions), key=lambda x: int(x.split('-')[0]))
    cache = read_cache(cache_filepath)
    pbar = tqdm(total=len(versions))
    for version in versions:
        changes: Dict[URIRef, dict] = dict()
        processed_files = []
        for operation in ['added', 'deleted']:
            operation_file = f'data-{operation}_{version}.nt.gz'
            if operation_file not in cache['processed_files']:
                accumulated_sujects = list()
                gzip_file_path = os.path.join(cb_dir, operation_file)
                number_of_lines = raw_newline_count_gzip(gzip_file_path)
                with gzip.open(gzip_file_path,'rt') as f:
                    print(operation_file)
                    pbar_1 = tqdm(total=number_of_lines)
                    for i, line in enumerate(f):
                        triple = line.split(' ')[:-1]
                        subject = URIRef(triple[0][1:-1])
                        predicate = URIRef(triple[1][1:-1])
                        obj = URIRef(triple[2][1:-1]) if triple[2].startswith('<') else Literal(triple[2])
                        changes.setdefault(subject, {'added': list(), 'deleted': list()})
                        changes[subject][operation].append((predicate, obj))
                        accumulated_sujects.append(subject)
                        if i > 0 and i % 100 == 0:
                            try:
                                Reader.import_entities_from_triplestore(ocdm_graph, ts_url, accumulated_sujects)
                            except ValueError:
                                pass
                            accumulated_sujects = list()
                        pbar_1.update()
                    pbar_1.close()
                processed_files.append(operation_file)
        ocdm_graph.preexisting_finished('https://orcid.org/0000-0002-8420-0696')
        for entity, modifications in changes.items():
            for triple in modifications['added']:
                ocdm_graph.add((entity, triple[0], triple[1]))
            for triple in modifications['deleted']:
                ocdm_graph.remove((entity, triple[0], triple[1]))
        ocdm_graph.generate_provenance()
        storer = Storer(ocdm_graph)
        prov_storer = Storer(ocdm_graph.provenance)
        storer.upload_all(ts_url, base_dir)
        prov_storer.upload_all(ts_url, base_dir)
        ocdm_graph.commit_changes()
        update_cache(cache_filepath, cache, processed_files)
        pbar.update()
    pbar.close()


sqllite_counter_handler = SqliteCounterHandler('C:/Users/arcangelo.massari2/Desktop/time-agnostic-library/bear/counter.db')
# filesystem_counter_handler = FilesystemCounterHandler(info_dir='C:/Users/arcangelo.massari2/Desktop/time-agnostic-library/bear/info_dir')
generate_ocdm_provenance('E:/CB', 'http://192.168.10.23:29999/blazegraph/sparql', sqllite_counter_handler, 'C:/Users/arcangelo.massari2/Desktop/time-agnostic-library/bear/generate_ocdm_provenance.json', 'C:/Users/arcangelo.massari2/Desktop/time-agnostic-library/bear')