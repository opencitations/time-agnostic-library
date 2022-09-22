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


from SPARQLWrapper import SPARQLWrapper
from time_agnostic_library.statistics import Statistics
from typing import Tuple
import json
import os
import timeit


OUTPUT_PATH = 'statistics.json'
TRIPLESTORES = ['virtuoso']
entities = [2, 12935, 90837, 96505, 140957, 21821, 16973, 30948, 32497, 32544, 33243, 99238, 26565, 62320, 18046, 49, 141031, 69559, 32787, 86766]


def create_statistics_file(output_path:str=OUTPUT_PATH, entities=entities):
    if not os.path.isfile(output_path):
        with open(output_path, 'w') as f:
            statistics_data = {'time': dict(), 'memory': dict()}
            queries = ['materialization_all_versions', 'materialization_single_version', 'cross_version_structured_query', 'single_version_structured_query', 
            'cross_version_structured_query_p_o', 'single_version_structured_query_p_o', 'cross_delta_structured_query', 'single_delta_structured_query', 
            'cross_delta_structured_query_p_o', 'single_delta_structured_query_p_o']
            time_parameters = ['w_out_cache_w_out_index', 'w_out_cache_w_index', 'w_cache_w_out_index', 'w_cache_w_index']
            memory_parameters = ['w_cache_w_out_index', 'w_out_cache_w_out_index']
            for benchmark in dict(statistics_data):
                parameters = time_parameters if benchmark == 'time' else memory_parameters
                for query in queries:
                    statistics_data[benchmark].setdefault(query, dict())
                    if 'p_o' in query:
                        statistics_data[benchmark][query].setdefault('p_o', dict())
                        for parameter in parameters:
                            statistics_data[benchmark][query]['p_o'][parameter] = list()
                    else:
                        for entity in entities:
                            entity_name = f':br/{str(entity)}'
                            statistics_data[benchmark][query].setdefault(entity_name, dict())
                            for parameter in parameters:
                                statistics_data[benchmark][query][entity_name][parameter] = list()
            json.dump(statistics_data, f, indent=4)

def save(data:int, test_name:str, parameter:str, benchmark:str, entity:str, output_path:str=OUTPUT_PATH): 
    with open(output_path, 'r', encoding='utf-8') as reader:
        prev_data = json.load(reader)
    with open(output_path, 'w', encoding='utf-8') as writer:
        prev_data[benchmark][test_name][entity][parameter].append(data)
        json.dump(prev_data, writer, indent=4)

def already_done(parameter:str, test_name:str, benchmark:str, entity:str, repetitions:int, output_path:str=OUTPUT_PATH) -> int:
    triplestore = output_path.replace('config_', '').replace('.json', '')
    with open(output_path, 'r', encoding='utf-8') as reader:
        prev_data = json.load(reader)
        work_done = len(prev_data[benchmark][test_name][entity][parameter])
        if work_done == repetitions:
            print(f'{triplestore}: benchmark on {benchmark} for {entity} completed: {test_name} - {parameter}')
    return work_done

def save_baseline(setup:str, test:str, benchmark:str, test_name:str, entity:str, output_path:str, config_filepath:str):
    if benchmark == 'time':
        code = setup + '\n' + test.replace('include_prov_metadata=False', 'include_prov_metadata=True')
        with open(output_path, 'r', encoding='utf-8') as reader:
            prev_data = json.load(reader)
        path = prev_data[benchmark][test_name][entity]
        if 'overhead' not in path or 'baseline' not in path:
            exec(code)
        if 'overhead' not in path:
            statistics_obj = Statistics(statistics)
            overhead = statistics_obj.get_overhead()
            with open(output_path, 'w', encoding='utf-8') as writer:
                prev_data[benchmark][test_name][entity]['overhead'] = overhead
                json.dump(prev_data, writer, indent=4)
        if 'baseline' not in path:
            with open(config_filepath, 'r', encoding='utf-8') as reader:
                config_data = json.load(reader)
                endpoints = config_data['dataset']['triplestore_urls']
            for endpoint in endpoints:
                sparql = SPARQLWrapper(endpoint)
                sparql.setQuery(query)
                times_on_current_snapshot = timeit.repeat(sparql.queryAndConvert, setup=setup, repeat=10, number=1)
            with open(output_path, 'w', encoding='utf-8') as writer:
                prev_data[benchmark][test_name][entity]['baseline'] = times_on_current_snapshot
                json.dump(prev_data, writer, indent=4)

def get_setup(triplestore:str) -> Tuple[str, str]:
    setup = '''
from time_agnostic_library.agnostic_entity import AgnosticEntity
from time_agnostic_library.agnostic_query import VersionQuery, DeltaQuery
from triplestore_manager import TriplestoreManager
import time

triplestore_manager = TriplestoreManager()
    '''
    if triplestore.lower() == 'blazegraph':
        setup += '''
triplestore_manager.close_program_by_name('java')
triplestore_manager.launch_blazegraph('./db/blazegraph', 9999)\n'''
        setup_no_cache = setup + 'time.sleep(10)'
        setup_cache = setup + f"triplestore_manager.launch_blazegraph('./db/blazegraph/cache', 29999)\ntime.sleep(10)"
    elif triplestore.lower() == 'graphdb':
        setup += '''
triplestore_manager.close_program_by_name('java')
triplestore_manager.launch_graphdb('./db/graphdb', 7200)
\n'''
        setup_no_cache = setup + 'time.sleep(10)'
        setup_cache = setup_no_cache
    elif triplestore.lower() == 'fuseki':
        setup += '''
triplestore_manager.close_program_by_name('java')
triplestore_manager.launch_fuseki(3030)
\n'''
        setup_no_cache = setup + 'time.sleep(10)'
        setup_cache = setup_no_cache
    elif triplestore.lower() == 'virtuoso':
        setup += '''
triplestore_manager.close_program_by_name('virtuoso-t')
triplestore_manager.launch_virtuoso('database')
\n'''
        setup_no_cache = setup + 'time.sleep(10)'
        setup_cache = setup + f"triplestore_manager.launch_virtuoso('cache_db')\ntime.sleep(10)"
    return setup_no_cache, setup_cache

def generate_tests_for_entities(test:str, entities:list) -> list:
    tests_for_entities = list()
    for entity in entities:
        tests_for_entities.append(test.replace('{entity}', str(entity)))
    return tests_for_entities

materialization_all_versions = """
agnostic_entity = AgnosticEntity(res='https://github.com/opencitations/time-agnostic-library/br/{entity}', config_path=CONFIG)
global statistics
statistics, _ = agnostic_entity.get_history(include_prov_metadata=False)
global query
query = '''
    SELECT DISTINCT ?s ?p ?o ?c
    WHERE {
        BIND (<https://github.com/opencitations/time-agnostic-library/br/{entity}> AS ?s) 
        GRAPH ?c {?s ?p ?o}
    }
'''
"""
materialization_single_version = """
agnostic_entity = AgnosticEntity(res='https://github.com/opencitations/time-agnostic-library/br/{entity}', config_path=CONFIG)
_, entity_snapshots, other_snapshots = agnostic_entity.get_state_at_time(time=('2021-10-13T21:32:16', '2021-10-13T21:32:16'), include_prov_metadata=False)
global statistics
statistics = (entity_snapshots, other_snapshots)
global query
query = '''
    SELECT DISTINCT ?s ?p ?o ?c
    WHERE {
        BIND (<https://github.com/opencitations/time-agnostic-library/br/{entity}> AS ?s) 
        GRAPH ?c {?s ?p ?o}
    }
'''
"""
cross_version_structured_query = """
global query
query = '''
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX cito: <http://purl.org/spar/cito/>
PREFIX datacite: <http://purl.org/spar/datacite/> 
SELECT DISTINCT ?br ?id ?value
WHERE {
    <https://github.com/opencitations/time-agnostic-library/br/{entity}> cito:cites ?br.
    ?br datacite:hasIdentifier ?id.
    OPTIONAL {?id literal:hasLiteralValue ?value.}
}   
'''
agnostic_query = VersionQuery(query, config_path=CONFIG)
global statistics
agnostic_query.run_agnostic_query()
statistics = agnostic_query.relevant_entities_graphs
"""
single_version_structured_query = """
global query
query = '''
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX cito: <http://purl.org/spar/cito/>
PREFIX datacite: <http://purl.org/spar/datacite/> 
SELECT DISTINCT ?br ?id ?value
WHERE {
    <https://github.com/opencitations/time-agnostic-library/br/{entity}> cito:cites ?br.
    ?br datacite:hasIdentifier ?id.
    OPTIONAL {?id literal:hasLiteralValue ?value.}
}   
'''
agnostic_query = VersionQuery(query, ('2021-10-13T21:32:16', '2021-10-13T21:32:16'), config_path=CONFIG)
global statistics
agnostic_query.run_agnostic_query()
statistics = agnostic_query.relevant_entities_graphs
"""
cross_delta_structured_query = """
global query
query = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX oco: <https://w3id.org/oc/ontology/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX cito: <http://purl.org/spar/cito/>
SELECT DISTINCT ?br ?id
WHERE {
    <https://github.com/opencitations/time-agnostic-library/br/{entity}> cito:cites ?br.
    ?br datacite:hasIdentifier ?id.
    ?se_br prov:specializationOf ?br; 
        prov:wasDerivedFrom ?prev_se_br.
    ?se_id prov:specializationOf ?id; 
        prov:wasDerivedFrom ?prev_se_id.
}
'''
query_delta = '''
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX cito: <http://purl.org/spar/cito/>
PREFIX datacite: <http://purl.org/spar/datacite/> 
SELECT DISTINCT ?br ?id ?value
WHERE {
    <https://github.com/opencitations/time-agnostic-library/br/{entity}> cito:cites ?br.
    ?br datacite:hasIdentifier ?id.
    OPTIONAL {?id literal:hasLiteralValue ?value.}
}
'''
agnostic_entity = DeltaQuery(
    query=query_delta, 
    config_path=CONFIG
)
agnostic_entity.run_agnostic_query()
global statistics
statistics = agnostic_entity.reconstructed_entities
"""
single_delta_structured_query = """
global query
query = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX oco: <https://w3id.org/oc/ontology/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX cito: <http://purl.org/spar/cito/>
SELECT DISTINCT ?br ?id
WHERE {
    <https://github.com/opencitations/time-agnostic-library/br/{entity}> cito:cites ?br.
    ?br datacite:hasIdentifier ?id.
    ?se_br prov:specializationOf ?br; 
        prov:wasDerivedFrom ?prev_se_br.
    ?se_id prov:specializationOf ?id; 
        prov:wasDerivedFrom ?prev_se_id.
}
'''
query_delta = '''
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX cito: <http://purl.org/spar/cito/>
PREFIX datacite: <http://purl.org/spar/datacite/> 
SELECT DISTINCT ?br ?id ?value
WHERE {
    <https://github.com/opencitations/time-agnostic-library/br/{entity}> cito:cites ?br.
    ?br datacite:hasIdentifier ?id.
    OPTIONAL {?id literal:hasLiteralValue ?value.}
}
'''
agnostic_entity = DeltaQuery(
    query=query_delta, 
    on_time=('2021-10-13T21:32:16', '2021-10-13T21:32:16'),
    config_path=CONFIG
)
agnostic_entity.run_agnostic_query()
global statistics
statistics = agnostic_entity.reconstructed_entities
"""    

cross_version_structured_query_p_o = """
global query
query = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
SELECT DISTINCT ?s
WHERE {
    ?s datacite:usesIdentifierScheme datacite:orcid.
}
'''
agnostic_query = VersionQuery(query, config_path=CONFIG)
global statistics
agnostic_query.run_agnostic_query()
statistics = agnostic_query.relevant_entities_graphs
"""

single_version_structured_query_p_o = """
global query
query = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
SELECT DISTINCT ?s
WHERE {
    ?s datacite:usesIdentifierScheme datacite:orcid.
}
'''
agnostic_query = VersionQuery(query, ('2021-10-14T22:24:21', '2021-10-14T22:24:21'), config_path=CONFIG)
global statistics
agnostic_query.run_agnostic_query()
statistics = agnostic_query.relevant_entities_graphs
"""

single_delta_structured_query_p_o = """
global query
query = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX oco: <https://w3id.org/oc/ontology/>
PREFIX prov: <http://www.w3.org/ns/prov#>
SELECT DISTINCT ?id
WHERE {
    ?id datacite:usesIdentifierScheme datacite:orcid.
    ?se prov:specializationOf ?id; 
        prov:wasDerivedFrom ?prev_se.
}
'''
query_delta = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
SELECT DISTINCT ?s
WHERE {
    ?s datacite:usesIdentifierScheme datacite:orcid.
}
'''
agnostic_entity = DeltaQuery(
    query=query_delta,
    on_time=('2021-10-14T22:24:21', '2021-10-14T22:24:21'),
    config_path=CONFIG
)
agnostic_entity.run_agnostic_query()
global statistics
statistics = agnostic_entity.reconstructed_entities
"""

cross_delta_structured_query_p_o = """
global query
query = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX oco: <https://w3id.org/oc/ontology/>
PREFIX prov: <http://www.w3.org/ns/prov#>
SELECT DISTINCT ?id
WHERE {
    ?id datacite:usesIdentifierScheme datacite:orcid.
    ?se prov:specializationOf ?id; 
        prov:wasDerivedFrom ?prev_se.
}
'''
query_delta = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
SELECT DISTINCT ?s
WHERE {
    ?s datacite:usesIdentifierScheme datacite:orcid.
}
'''
agnostic_entity = DeltaQuery(
    query=query_delta,
    config_path=CONFIG
)
agnostic_entity.run_agnostic_query()
global statistics
statistics = agnostic_entity.reconstructed_entities
"""

tests_time_w_out_cache_w_out_index = {
    'materialization_all_versions': generate_tests_for_entities(test=materialization_all_versions, entities=entities),
    'materialization_single_version': generate_tests_for_entities(test=materialization_single_version, entities=entities),
    'cross_version_structured_query': generate_tests_for_entities(test=cross_version_structured_query, entities=entities),
    'single_version_structured_query': generate_tests_for_entities(test=single_version_structured_query, entities=entities),
    'cross_version_structured_query_p_o': [cross_version_structured_query_p_o],
    'single_version_structured_query_p_o': [single_version_structured_query_p_o],
    'cross_delta_structured_query': generate_tests_for_entities(test=cross_delta_structured_query, entities=entities),
    'single_delta_structured_query': generate_tests_for_entities(test=single_delta_structured_query, entities=entities),
    'cross_delta_structured_query_p_o': [cross_delta_structured_query_p_o],
    'single_delta_structured_query_p_o': [single_delta_structured_query_p_o]
}

tests_time_w_out_cache_w_index = {
    'cross_version_structured_query_p_o': [cross_version_structured_query_p_o],
    'single_version_structured_query_p_o': [single_version_structured_query_p_o],
    'cross_delta_structured_query_p_o': [cross_delta_structured_query_p_o],
    'single_delta_structured_query_p_o': [single_delta_structured_query_p_o]
}

tests_time_w_cache_w_out_index = {
    'cross_version_structured_query': generate_tests_for_entities(test=cross_version_structured_query, entities=entities),
    'single_version_structured_query': generate_tests_for_entities(test=single_version_structured_query, entities=entities),
    'cross_version_structured_query_p_o': [cross_version_structured_query_p_o],
    'single_version_structured_query_p_o': [single_version_structured_query_p_o]
}

tests_time_w_cache_w_index = {
    'cross_version_structured_query_p_o': [cross_version_structured_query_p_o],
    'single_version_structured_query_p_o': [single_version_structured_query_p_o]
}

tests_memory_w_out_cache_w_out_index = {
    'materialization_all_versions': generate_tests_for_entities(test=materialization_all_versions, entities=entities),
    'materialization_single_version': generate_tests_for_entities(test=materialization_single_version, entities=entities),
    'cross_version_structured_query': generate_tests_for_entities(test=cross_version_structured_query, entities=entities),
    'single_version_structured_query': generate_tests_for_entities(test=single_version_structured_query, entities=entities),
    'cross_version_structured_query_p_o': [cross_version_structured_query_p_o],
    'single_version_structured_query_p_o': [single_version_structured_query_p_o],
    'cross_delta_structured_query': generate_tests_for_entities(test=cross_delta_structured_query, entities=entities),
    'single_delta_structured_query': generate_tests_for_entities(test=single_delta_structured_query, entities=entities),
    'cross_delta_structured_query_p_o': [cross_delta_structured_query_p_o],
    'single_delta_structured_query_p_o': [single_delta_structured_query_p_o]
}

tests_memory_w_cache_w_out_index = {
    'cross_version_structured_query': generate_tests_for_entities(test=cross_version_structured_query, entities=entities),
    'single_version_structured_query': generate_tests_for_entities(test=single_version_structured_query, entities=entities),
    'cross_version_structured_query_p_o': [cross_version_structured_query_p_o],
    'single_version_structured_query_p_o': [single_version_structured_query_p_o]
}

