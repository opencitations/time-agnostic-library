from __future__ import annotations

import glob
import gzip
import json
import os
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_values
from rdflib import ConjunctiveGraph, URIRef
from rdflib_ocdm.counter_handler.redis_counter_handler import \
    RedisCounterHandler
from rdflib_ocdm.prov.prov_entity import ProvEntity
from rdflib_ocdm.prov.snapshot_entity import SnapshotEntity
from rdflib_ocdm.query_utils import get_delete_query, get_insert_query
from rdflib_ocdm.support import get_prov_count
from tqdm import tqdm


def is_uri(stringa):
    try:
        urlparse(stringa)
        return True
    except ValueError:
        return False

def generate_ocdm_provenance(cb_dir: str, cache_filepath: str, counter_handler: RedisCounterHandler, psyco_cursor, psyco_connection, source: str, resp_agent: str, output_dir: str):
    versions = set()
    for filename in os.listdir(cb_dir):
        versions.add(filename.split('_')[1].replace('.nt.gz', ''))
    versions = sorted(list(versions), key=lambda x: int(x.split('-')[0]))
    cache = read_cache(cache_filepath)
    pbar = tqdm(total=len(versions))
    for version in versions:
        changes: Dict[URIRef, dict] = dict()
        processed_files = []
        start_time = time.time()
        cur_time: str = datetime.fromtimestamp(start_time, tz=timezone.utc).replace(microsecond=0).isoformat(sep="T")
        all_entities = set()
        for operation in ['added', 'deleted']:
            operation_file = f'data-{operation}_{version}.nt.gz'
            if operation_file in cache['processed_files']:
                continue
            gzip_file_path = os.path.join(cb_dir, operation_file)
            number_of_lines = raw_newline_count_gzip(gzip_file_path)
            pbar_file = tqdm(total=number_of_lines)
            with gzip.open(gzip_file_path, 'rt') as f:
                for i, line in enumerate(f):
                    triple = line.split(' ')[:-1]
                    subject = triple[0]
                    predicate = triple[1]
                    obj = triple[2]
                    changes.setdefault(f'{subject}_{operation}', list())
                    changes[f'{subject}_{operation}'].append((predicate, obj))
                    all_entities.add(subject)
                    pbar_file.update()
                serialized_changes = [(urllib.parse.quote((k)), json.dumps(v)) for k, v in changes.items()]
                query = "INSERT INTO changes (key, value) VALUES %s"
                execute_values(psyco_cursor, query, serialized_changes)
                psyco_connection.commit()
                changes = dict()
            pbar_file.close()
            processed_files.append(operation_file)
        all_prov_counters = [int(counter.decode('utf8')) if counter else counter for counter in counter_handler.connection.mget(all_entities)]
        existing_snapshots = {entity: all_prov_counters[i] for i, entity in enumerate(all_entities)}
        updated_snapshots = dict()
        pbar_provenance = tqdm(total=len(all_entities))
        batch_size = 1000000
        entities_batch = [list(all_entities)[i:i + batch_size] for i in range(0, len(all_entities), batch_size)]

        for entity_batch in entities_batch:
            ocdm_prov = OCDMProvenance(
                source=source, resp_agent=resp_agent, counter_handler=counter_handler)

            modifications_added = get_modifications('added', entity_batch, psyco_cursor)
            modifications_deleted = get_modifications('deleted', entity_batch, psyco_cursor)

            for entity in entity_batch:
                entity = URIRef(entity[1:-1])
                last_snapshot_res = ocdm_prov.retrieve_last_snapshot(entity, existing_snapshots)

                if last_snapshot_res is None:
                    # CREATION SNAPSHOT
                    cur_snapshot: SnapshotEntity = ocdm_prov.create_snapshot(entity, cur_time, existing_snapshots, updated_snapshots)
                    cur_snapshot.has_description(f"The entity '{str(entity)}' has been created.")
                else:
                    # MODIFICATION SNAPSHOT
                    last_snapshot: SnapshotEntity = ocdm_prov.add_se(prov_subject=entity, res=last_snapshot_res, existing_snapshots=existing_snapshots, updated_snapshots=updated_snapshots)
                    last_snapshot.has_invalidation_time(cur_time)
                    cur_snapshot: SnapshotEntity = ocdm_prov.create_snapshot(entity, cur_time, existing_snapshots, updated_snapshots)
                    cur_snapshot.derives_from(last_snapshot)
                    cur_snapshot.has_description(f"The entity '{str(entity)}' was modified.")
                    formatted_entity = f'<{str(entity)}>'
                    insert_query = ''
                    if formatted_entity in modifications_added:
                        if modifications_added[formatted_entity]:
                            insert_query, _ = get_insert_query(entity, modifications_added[formatted_entity])
                    delete_query = ''
                    if formatted_entity in modifications_deleted:
                        if modifications_deleted[formatted_entity]:
                            delete_query, _ = get_delete_query(entity, modifications_deleted[formatted_entity])
                    update_query = ocdm_prov.get_update_query(insert_query, delete_query)
                    cur_snapshot.has_update_action(update_query)
                pbar_provenance.update()

            save_graphs_to_nquads(ocdm_prov, output_dir)

        psyco_cursor.execute("TRUNCATE TABLE changes")
        psyco_connection.commit()
        pbar_provenance.close()
        if updated_snapshots:
            counter_handler.connection.mset(updated_snapshots)
        end_time = time.time()
        elapsed_time = end_time - start_time
        update_cache(cache_filepath, cache,
                     processed_files, version, elapsed_time)
        pbar.update()
    pbar.close()

def build_statements(entity, modifications: List[Tuple[str, str, str]]) -> str:
    statements = ''
    for modification in modifications:
        statements += f"<{entity}> {modification[0]} {modification[1]} . "
    return statements

def get_insert_query(entity: str, modifications: List[Tuple[str, str, str]], graph_iri: str = None) -> Tuple[str, int]:
    num_of_statements = len(modifications)
    if num_of_statements <= 0:
        return "", 0
    else:
        statements = build_statements(entity, modifications)
        if graph_iri:
            insert_string = f"INSERT DATA {{ GRAPH <{graph_iri}> {{ {statements} }} }}"
        else:
            insert_string = f"INSERT DATA {{ {statements} }}"
        return insert_string, num_of_statements

def get_delete_query(entity: str, modifications: List[Tuple[str, str, str]], graph_iri: str = None) -> Tuple[str, int]:
    num_of_statements = len(modifications)
    if num_of_statements <= 0:
        return "", 0
    else:
        statements = build_statements(entity, modifications)
        if graph_iri:
            delete_string = f"DELETE DATA {{ GRAPH <{graph_iri}> {{ {statements} }} }}"
        else:
            delete_string = f"DELETE DATA {{ {statements} }}"
        return delete_string, num_of_statements

def get_modifications(operation: str, entities: list, cursor) -> list:
    quoted_entities = ', '.join(f"'{urllib.parse.quote(entity)}_{operation}'" for entity in entities)
    query = f"SELECT key, value FROM changes WHERE key IN ({quoted_entities})"
    cursor.execute(query)
    results = cursor.fetchall()
    modifications = {urllib.parse.unquote(key).replace(f'_{operation}', ''): json.loads(value) for key, value in results} if results else {}
    return modifications

def raw_newline_count_gzip(fname):
    f = gzip.open(fname, 'rb')
    f_gen = _reader_generator(f.read)
    return sum(buf.count(b'\n') for buf in f_gen)


def _reader_generator(reader):
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024 * 1024)


def update_cache(cache_filepath: str, data: dict, processed_files: list, version: str, elapsed_time: int) -> None:
    cache = read_cache(cache_filepath)
    if all(processed_file in cache['processed_files'] for processed_file in processed_files):
        return
    data['processed_files'].extend(processed_files)
    data['processing_times'][version] = elapsed_time
    with open(cache_filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f)

def read_cache(cache_filepath: str) -> dict:
    if os.path.exists(cache_filepath):
        with open(cache_filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = {'processed_files': [], 'processing_times': {}}
    return data


class OCDMProvenance(ConjunctiveGraph):
    def __init__(self, source: URIRef, resp_agent: URIRef, counter_handler: RedisCounterHandler):
        ConjunctiveGraph.__init__(self)
        self.counter_handler = counter_handler
        self.res_to_entity: Dict[URIRef, ProvEntity] = dict()
        self.source = source
        self.resp_agent = resp_agent
        self.all_entities = set()

    @staticmethod
    def get_update_query(insert_string: str, delete_string: str) -> str:
        if delete_string != "" and insert_string != "":
            return delete_string + '; ' + insert_string
        elif delete_string != "":
            return delete_string
        elif insert_string != "":
            return insert_string
        else:
            return ""

    def retrieve_last_snapshot(self, prov_subject: URIRef, existing_snapshots: dict) -> Optional[URIRef]:
        last_snapshot_count = existing_snapshots[f'<{prov_subject}>']
        if last_snapshot_count is None:
            return None
        else:
            return URIRef(prov_subject + '/prov/se/' + str(last_snapshot_count))

    def create_snapshot(self, cur_subj: URIRef, cur_time: str, existing_snapshots: dict, updated_snapshots: dict) -> SnapshotEntity:
        new_snapshot: SnapshotEntity = self.add_se(prov_subject=cur_subj, existing_snapshots=existing_snapshots, updated_snapshots=updated_snapshots)
        new_snapshot.is_snapshot_of(cur_subj)
        new_snapshot.has_generation_time(cur_time)
        new_snapshot.has_primary_source(URIRef(self.source))
        new_snapshot.has_resp_agent(URIRef(self.resp_agent))
        return new_snapshot

    def add_se(self, prov_subject: URIRef, res: URIRef = None, existing_snapshots: dict = dict(), updated_snapshots: dict = dict()) -> SnapshotEntity:
        if res is not None and res in self.res_to_entity:
            return self.res_to_entity[res]
        count = self.add_prov(str(prov_subject), res, existing_snapshots, updated_snapshots)
        se = SnapshotEntity(str(prov_subject), self, count)
        return se

    def add_prov(self, prov_subject: str, res: URIRef, existing_snapshots: dict, updated_snapshots: dict) -> Optional[str]:
        if res is not None:
            res_count: int = int(get_prov_count(res))
            if res_count > existing_snapshots[f'<{prov_subject}>']:
                updated_snapshots[f'<{prov_subject}>'] = res_count
            return str(res_count)
        res_count = existing_snapshots[f'<{prov_subject}>']
        if res_count is None:
            res_count = 0
        res_count += 1
        updated_snapshots[f'<{prov_subject}>'] = res_count
        return str(res_count)

def save_graphs_to_nquads(ocdm_prov: OCDMProvenance, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)

    existing_files = glob.glob(os.path.join(output_dir, "graph_*.nq"))
    existing_files.sort(key=lambda f: int(os.path.basename(os.path.normpath(f)).split("_")[1].split(".")[0]))

    if existing_files:
        last_file = os.path.basename(os.path.normpath(existing_files[-1]))
        file_counter = int(last_file.split("_")[1].split(".")[0]) + 1
    else:
        file_counter = 1

    triple_counter = 0
    current_graph = ConjunctiveGraph()

    for triple in ocdm_prov.triples((None, None, None)):
        context = URIRef(str(triple[0]).split('se/')[0])
        quad = (triple[0], triple[1], triple[2], context)
        current_graph.add(quad)
        triple_counter += 1

        if triple_counter >= 10000:
            output_file = os.path.join(output_dir, f"graph_{file_counter}.nq")
            current_graph.serialize(destination=output_file, format='nquads', encoding='utf-8')

            triple_counter = 0
            del current_graph
            current_graph = ConjunctiveGraph()

            file_counter += 1

    if triple_counter > 0:
        output_file = os.path.join(output_dir, f"graph_{file_counter}.nq")
        current_graph.serialize(destination=output_file, format='nquads', encoding='utf-8')


redis_counter_handler = RedisCounterHandler(host='127.0.0.1', port=6379, db=0)
db_host = 'localhost'
db_port = '5432'
db_name = 'bear'
db_user = 'postgres'
db_password = 'Permesiva1!'

psyco = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password
)
psyco_cursor = psyco.cursor()
psyco_cursor.execute('''CREATE TABLE IF NOT EXISTS changes(
    key TEXT PRIMARY KEY,
    value TEXT
)''')
psyco.commit()
psyco_cursor.execute("TRUNCATE TABLE changes")
psyco.commit()

redis_counter_handler.connect()
generate_ocdm_provenance('E:/CB', 'E:/generate_ocdm_provenance.json', redis_counter_handler, psyco_cursor, psyco, URIRef('https://aic.ai.wu.ac.at/qadlod/bear/BEAR_A/datasets/CB/alldata.CB.nt.tar.gz'), URIRef('https://orcid.org/0000-0002-8420-0696'), 'E:/provenance_files')
redis_counter_handler.disconnect()