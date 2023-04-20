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


def generate_ocdm_provenance(cb_dir: str, ts_url: str, counter_handler: CounterHandler, base_dir: str = None):
    ocdm_graph = OCDMGraph(counter_handler)
    versions = set()
    for filename in os.listdir(cb_dir):
        versions.add(filename.split('_')[1].replace('.nt.gz', ''))
    versions = sorted(list(versions), key=lambda x: int(x.split('-')[0]))
    pbar = tqdm(total=len(versions))
    for version in versions:
        changes: Dict[URIRef, dict] = dict()
        for operation in ['added', 'deleted']:
            operation_file = f'data-{operation}_{version}.nt.gz'
            with gzip.open(os.path.join(cb_dir, operation_file),'rt') as f:
                for line in f:
                    triple = line.split(' ')[:-1]
                    subject = URIRef(triple[0][1:-1])
                    try:
                        Reader.import_entity_from_triplestore(ocdm_graph, ts_url, subject)
                    except ValueError:
                        pass
                    predicate = URIRef(triple[1][1:-1])
                    obj = URIRef(triple[2][1:-1]) if triple[2].startswith('<') else Literal(triple[2])
                    changes.setdefault(subject, {'added': list(), 'deleted': list()})
                    changes[subject][operation].append((predicate, obj))
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
        pbar.update()
    pbar.close()


sqllite_counter_handler = SqliteCounterHandler('C:/Users/arcangelo.massari2/Desktop/time-agnostic-library/bear/counter.db')
generate_ocdm_provenance('E:/CB', 'http://192.168.10.23:9999/blazegraph/sparql', sqllite_counter_handler, 'E:/generate_ocdm_provenance')