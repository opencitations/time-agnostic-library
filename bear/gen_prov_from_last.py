from __future__ import annotations

import argparse
import glob
import gzip
import json
import os
import re
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import psycopg2
from psycopg2.extras import execute_values
from rdflib import ConjunctiveGraph, Graph, Literal, URIRef
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

def parse_triple(line):
    match = re.match(r"(<.+?>) (<.+?>) (.+?)\s?\.$", line)
    if match:
        subject = match.group(1)
        predicate = match.group(2)
        obj = match.group(3)
        return subject, predicate, obj
    else:
        raise ValueError(f"Impossibile analizzare la tripla: {line}")

def read_rdf_gzip_file(gzip_file_path: str):
    g = Graph()
    with gzip.open(gzip_file_path, 'rt') as f:
        g.parse(f, format='nt')
    return g

def serialize_modifications(modifications):
    serialized_modifications = []
    for predicate, obj in modifications:
        mod_dict = {
            "predicate": predicate, 
            "value": str(obj)
        }

        if isinstance(obj, URIRef):
            mod_dict["type"] = "uri"
        elif isinstance(obj, Literal):
            mod_dict["type"] = "literal"
            if obj.datatype:
                mod_dict["datatype"] = str(obj.datatype)
            if obj.language:
                mod_dict["language"] = obj.language

        serialized_modifications.append(json.dumps(mod_dict))
    return serialized_modifications

def deserialize_modifications(serialized_modifications):
    modifications = []
    serialized_modifications = json.loads(serialized_modifications)
    for serialized_modification in serialized_modifications:
        mod_dict: dict = json.loads(serialized_modification)
        predicate = mod_dict["predicate"]
        obj_type = mod_dict["type"]
        value = mod_dict["value"]

        if obj_type == "uri":
            obj = URIRef(value)
        elif obj_type == "literal":
            datatype = mod_dict.get("datatype")
            language = mod_dict.get("language")
            if datatype:
                obj = Literal(value, datatype=URIRef(datatype))
            elif language:
                obj = Literal(value, lang=language)
            else:
                obj = Literal(value)
        
        modifications.append((predicate, obj))
    return modifications

def process_initial_version(gzip_file_path: str, cb_dir: str, psyco_cursor, psyco_connection, counter_handler: RedisCounterHandler, source: str, resp_agent: str, output_dir: str, cache: Dict, in_memory_changes: Dict, in_memory_counters: Dict):
    cur_time: str = datetime.now(timezone.utc).isoformat(sep="T")
    graph = read_rdf_gzip_file(gzip_file_path)
    changes: Dict[URIRef, dict] = dict()
    all_entities = set()
    for subj, pred, obj in graph:
        subject = str(subj)
        key = f"{subject}_added"
        if psyco_cursor is not None and psyco_connection is not None:
            changes.setdefault(key, []).extend(serialize_modifications([(str(pred), obj)]))
        else:
            changes.setdefault(key, []).extend([(str(pred), obj)])
        all_entities.add(subject)
    if psyco_cursor is not None and psyco_connection is not None:
        serialized_changes = [(urllib.parse.quote((k)), json.dumps(v)) for k, v in changes.items()]
        query = "INSERT INTO changes (key, value) VALUES %s"
        execute_values(psyco_cursor, query, serialized_changes)
        psyco_connection.commit()
    else:
        in_memory_changes.update({urllib.parse.quote(k): v for k, v in changes.items()})
    process_changes(all_entities, psyco_cursor, psyco_connection, counter_handler, source, resp_agent, output_dir, cur_time, in_memory_changes, in_memory_counters)

def process_changes(all_entities: set, psyco_cursor, psyco_connection, counter_handler: RedisCounterHandler, source: str, resp_agent: str, output_dir: str, cur_time: str, in_memory_changes: Dict, in_memory_counters: Dict):    
    if counter_handler:
        all_prov_counters = [int(counter.decode('utf8')) if counter else counter for counter in counter_handler.connection.mget(all_entities)]
    else:
        all_prov_counters = [in_memory_counters.get(entity, None) for entity in all_entities]

    existing_snapshots = {e: all_prov_counters[i] for i, e in enumerate(all_entities)}
    updated_snapshots = dict()

    ocdm_prov = OCDMProvenance(source=URIRef(source), resp_agent=URIRef(resp_agent), counter_handler=counter_handler)

    batch_size = 1000000
    entities_batch = [list(all_entities)[i:i + batch_size] for i in range(0, len(all_entities), batch_size)]

    for entity_batch in entities_batch:
        if psyco_cursor is not None:
            modifications_added = get_modifications('added', entity_batch, psyco_cursor)
            modifications_deleted = get_modifications('deleted', entity_batch, psyco_cursor)
        else:
            modifications_added = get_in_memory_modifications('added', entity_batch, in_memory_changes)
            modifications_deleted = get_in_memory_modifications('deleted', entity_batch, in_memory_changes)
        for entity in entity_batch:
            entity_uri = URIRef(entity)
            last_snapshot_res = ocdm_prov.retrieve_last_snapshot(entity_uri, existing_snapshots)

            if last_snapshot_res is None:
                # CREATION SNAPSHOT for version 0 or first appearance of the entity
                cur_snapshot = ocdm_prov.create_snapshot(entity_uri, cur_time, existing_snapshots, updated_snapshots)
                cur_snapshot.has_description(f"The entity '{str(entity_uri)}' has been created.")
            else:
                # MODIFICATION SNAPSHOT for subsequent versions
                last_snapshot = ocdm_prov.add_se(prov_subject=entity_uri, res=last_snapshot_res, existing_snapshots=existing_snapshots, updated_snapshots=updated_snapshots)
                last_snapshot.has_invalidation_time(cur_time)
                cur_snapshot = ocdm_prov.create_snapshot(entity_uri, cur_time, existing_snapshots, updated_snapshots)
                cur_snapshot.derives_from(last_snapshot)
                cur_snapshot.has_description(f"The entity '{str(entity_uri)}' was modified.")
                formatted_entity = str(entity)
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

        save_graphs_to_nquads(ocdm_prov, output_dir)

    # Clear the changes after processing them
    if psyco_cursor is not None and psyco_connection is not None:
        psyco_cursor.execute("TRUNCATE TABLE changes")
        psyco_connection.commit()
    else:
        in_memory_changes.clear()

    # Update the snapshots counters
    if updated_snapshots:
        if counter_handler:
            counter_handler.connection.mset(updated_snapshots)
        else:
            in_memory_counters.update(updated_snapshots)

def generate_ocdm_provenance(cb_dir: str, initial_version_path: str, cache_filepath: str, counter_handler: RedisCounterHandler, psyco_cursor, psyco_connection, source: str, resp_agent: str, output_dir: str):
    in_memory_changes = dict()
    in_memory_counters = dict()
    cache = read_cache(cache_filepath)
    
    start_time = time.time()
    initial_version_file = os.path.basename(initial_version_path)
    if initial_version_file not in cache['processed_files']:
        process_initial_version(initial_version_path, cb_dir, psyco_cursor, psyco_connection, counter_handler, source, resp_agent, output_dir, cache, in_memory_changes, in_memory_counters)
        end_time = time.time()
        elapsed_time = end_time - start_time
        update_cache(cache_filepath, cache, [initial_version_file], '0-1', elapsed_time)

    versions = set()
    for filename in os.listdir(cb_dir):
        if filename.startswith('data-added_') or filename.startswith('data-deleted_'):
            version = filename.split('_')[1].replace('.nt.gz', '')
            versions.add(version)
    versions = sorted(list(versions), key=lambda x: int(x.split('-')[0]))

    pbar = tqdm(total=len(versions))
    last_time = None
    for version in versions:
        changes = dict()
        all_entities = set()
        processed_files = []
        start_time = time.time()

        while True:
            cur_time = datetime.now(timezone.utc).isoformat(sep="T")
            if cur_time != last_time:
                break
            time.sleep(0.001)
        last_time = cur_time

        for operation in ['added', 'deleted']:
            operation_file = f'data-{operation}_{version}.nt.gz'
            if operation_file in cache['processed_files']:
                continue 
            gzip_file_path = os.path.join(cb_dir, operation_file)
            graph = read_rdf_gzip_file(gzip_file_path)
            for subj, pred, obj in graph:
                subject = str(subj)
                key = f"{subject}_{operation}"
                if psyco_cursor is not None and psyco_connection is not None:
                    changes.setdefault(key, []).extend(serialize_modifications([(str(pred), obj)]))
                else:
                    changes.setdefault(key, []).extend([(str(pred), obj)])
                all_entities.add(subject)
            if psyco_cursor is not None and psyco_connection is not None:
                serialized_changes = [(urllib.parse.quote((k)), json.dumps(v)) for k, v in changes.items()]
                query = "INSERT INTO changes (key, value) VALUES %s"
                execute_values(psyco_cursor, query, serialized_changes)
                psyco_connection.commit()
            else:
                in_memory_changes.update({urllib.parse.quote(k): v for k, v in changes.items()})
            changes = dict()
            processed_files.append(operation_file)

        # Processa le modifiche raccolte per la versione corrente
        process_changes(all_entities, psyco_cursor, psyco_connection, counter_handler, source, resp_agent, output_dir, cur_time, in_memory_changes, in_memory_counters)
        end_time = time.time()
        elapsed_time = end_time - start_time
        update_cache(cache_filepath, cache, processed_files, version, elapsed_time)
        pbar.update()
    pbar.close()

    # Dopo l'elaborazione, assicurati di disconnettere eventuali gestori esterni come Redis
    if counter_handler:
        counter_handler.disconnect()
    if psyco_connection:
        psyco_cursor.close()
        psyco_connection.close()
  
def build_statements(entity, modifications: List[Tuple[str, URIRef | Literal]]) -> str:
    statements = ''
    for predicate, obj in modifications:
        if isinstance(obj, URIRef):
            obj_str = f"<{obj}>"
        elif isinstance(obj, Literal):
            obj_value = str(obj).replace('\\', '\\\\').replace('\"', '\\\"')
            obj_value = f'\"\"\"{obj_value}\"\"\"' if '\n' in obj_value else f"\"{obj_value}\""
            if obj.datatype:
                obj_str = f"{obj_value}^^<{obj.datatype}>"
            elif obj.language:
                obj_str = f"{obj_value}@{obj.language}"
            else:
                obj_str = f"{obj_value}"
        else:
            obj_value = str(obj).replace('\\', '\\\\').replace('\"', '\\\"')
            obj_value = f'\"\"\"{obj_value}\"\"\"' if '\n' in obj_value else f"\"{obj_value}\""
            obj_str = f"\"{obj_value}\""
        statements += f"<{entity}> <{predicate}> {obj_str} . "
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
    modifications = {urllib.parse.unquote(key).replace(f'_{operation}', ''): deserialize_modifications(value) for key, value in results} if results else {}
    return modifications

def get_in_memory_modifications(operation: str, entities: list, in_memory_changes: dict) -> dict:
    modifications = {}
    for entity in entities:
        key = f"{urllib.parse.quote(entity)}_{operation}"
        if key in in_memory_changes:
            modifications[entity] = in_memory_changes[key]
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
        last_snapshot_count = existing_snapshots[str(prov_subject)]
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
            if res_count > existing_snapshots[str(prov_subject)]:
                updated_snapshots[str(prov_subject)] = res_count
            return str(res_count)
        res_count = existing_snapshots[str(prov_subject)]
        if res_count is None:
            res_count = 0
        res_count += 1
        updated_snapshots[str(prov_subject)] = res_count
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


def main(args):
    redis_counter_handler = RedisCounterHandler(host=args.redis_host, port=args.redis_port, db=args.redis_db)

    if args.redis_host and args.redis_port and args.redis_db is not None:
        redis_counter_handler = RedisCounterHandler(host=args.redis_host, port=args.redis_port, db=args.redis_db)
        redis_counter_handler.connect()
        if not os.path.exists(args.cache_filepath):
            print("Il file di cache non esiste. Svuotamento del database Redis selezionato.")
            redis_counter_handler.connection.flushdb()
    else:
        redis_counter_handler = None
    if args.db_host and args.db_port and args.db_name and args.db_user and args.db_password:
        psyco = psycopg2.connect(
            host=args.db_host,
            port=args.db_port,
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password
        )
        psyco_cursor = psyco.cursor()
        psyco_cursor.execute('''CREATE TABLE IF NOT EXISTS changes(
            key TEXT PRIMARY KEY,
            value TEXT
        )''')
        psyco.commit()
        psyco_cursor.execute("TRUNCATE TABLE changes")
        psyco.commit()
    else:
        psyco = None
        psyco_cursor = None

    generate_ocdm_provenance(args.cb_dir, args.initial_version_path, args.cache_filepath, redis_counter_handler, psyco_cursor, psyco, URIRef(args.source), URIRef(args.resp_agent), args.output_dir)
    
    if redis_counter_handler:
        redis_counter_handler.disconnect()
    if psyco:
        psyco_cursor.close()
        psyco.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate OCDM Provenance")
    parser.add_argument("--cb_dir", required=True, help="Path to the CB directory")
    parser.add_argument("--initial_version_path", required=False, help="Path to the initial version's gzip N-Triples file")
    parser.add_argument("--cache_filepath", required=True, help="Path to the cache file")
    parser.add_argument("--redis_host", required=False, help="Redis host address")
    parser.add_argument("--redis_port", type=int, required=False, help="Redis port")
    parser.add_argument("--redis_db", type=int, required=False, help="Redis database number")
    parser.add_argument("--db_host", required=False, help="PostgreSQL host address")
    parser.add_argument("--db_port", required=False, help="PostgreSQL port")
    parser.add_argument("--db_name", required=False, help="PostgreSQL database name")
    parser.add_argument("--db_user", required=False, help="PostgreSQL user")
    parser.add_argument("--db_password", required=False, help="PostgreSQL password")
    parser.add_argument("--source", required=True, help="URI of the source")
    parser.add_argument("--resp_agent", required=True, help="URI of the responsible agent")
    parser.add_argument("--output_dir", required=True, help="Directory for output files")

    args = parser.parse_args()
    main(args)

# python3 -m bear.gen_prov_from_last --cb_dir /srv/data/arcangelo/bear/b/instant/ --cache_filepath /srv/data/arcangelo/bear/b/instant_ocdm/cache.json --redis_db 15 --db_name bear --db_host localhost --db_user arcangelo --db_port 5432 --db_password Permesiva1! --source https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B/datasets/instant/CB/alldata.CB.nt.tar.gz --resp_agent https://orcid.org/0000-0002-8420-0696 --output_dir /srv/data/arcangelo/bear/b/instant_ocdm/provenance_files