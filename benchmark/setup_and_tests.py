from typing import Tuple
import json
import os

OUTPUT_PATH = 'statistics.json'
TRIPLESTORES = ['blazegraph', 'graphdb', 'fuseki', 'virtuoso']

def create_statistics_file(output_path:str=OUTPUT_PATH):
    if not os.path.isfile(output_path):
        with open(output_path, 'w') as f:
            statistics_data = {'time': dict(), 'memory': dict()}
            queries = ['materialization_all_versions', 'materialization_single_version', 'cross_version_structured_query', 'single_version_structured_query', 
            'cross_version_structured_query_p_o', 'single_version_structured_query_p_o', 'cross_delta_structured_query', 'single_delta_structured_query', 
            'cross_delta_structured_query_p_o', 'single_delta_structured_query_p_o']
            time_parameters = ['w_out_cache_w_out_index', 'w_out_cache_w_index', 'w_cache_w_out_index', 'w_cache_w_index']
            memory_parameters = ['w_cache_w_out_index', 'w_out_cache_w_out_index']
            statistics = {'min': 0.0,'median': 0.0,'mean': 0.0,'stdev': 0.0,'max': 0.0}
            for benchmark in dict(statistics_data):
                parameters = time_parameters if benchmark == 'time' else memory_parameters
                for query in queries:
                    statistics_data[benchmark].setdefault(query, dict())
                    for parameter in parameters:
                        statistics_data[benchmark][query][parameter] = statistics
            json.dump(statistics_data, f, indent=4)

def save(data:dict, test_name:str, parameter:str, benchmark:str, output_path:str=OUTPUT_PATH): 
    with open(output_path, 'r', encoding='utf-8') as reader:
        prev_data = json.load(reader)
    with open(output_path, 'w', encoding='utf-8') as writer:
        prev_data[benchmark][test_name][parameter] = data
        json.dump(prev_data, writer, indent=4)

def already_done(parameter:str, test_name:str, benchmark:str, output_path:str=OUTPUT_PATH) -> bool:
    already_done = False
    triplestore = output_path.replace('config_', '').replace('.json', '')
    with open(output_path, 'r', encoding='utf-8') as reader:
        prev_data = json.load(reader)
        if prev_data[benchmark][test_name][parameter] != {'min': 0.0,'median': 0.0,'mean': 0.0,'stdev': 0.0,'max': 0.0}:
            already_done = True
            print(f'{triplestore}: benchmark on {benchmark} completed: {test_name} - {parameter}')
    return already_done

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

materialization_all_versions = '''
agnostic_entity = AgnosticEntity(res='https://github.com/opencitations/time-agnostic-library/br/{entity}', config_path=CONFIG)
agnostic_entity.get_history(include_prov_metadata=True)
'''
materialization_single_version = '''
agnostic_entity = AgnosticEntity(res='https://github.com/opencitations/time-agnostic-library/br/{entity}', config_path=CONFIG)
agnostic_entity.get_state_at_time(time=('2021-10-13', None), include_prov_metadata=True)
'''
cross_version_structured_query = """
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
agnostic_query.run_agnostic_query()
"""
single_version_structured_query = """
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
agnostic_query = VersionQuery(query, ('2021-10-13', None), config_path=CONFIG)
agnostic_query.run_agnostic_query()
"""
cross_delta_structured_query = """
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
agnostic_entity = DeltaQuery(
    query=query, 
    config_path=CONFIG
)
agnostic_entity.run_agnostic_query()
"""
single_delta_structured_query = """
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
agnostic_entity = DeltaQuery(
    query=query, 
    on_time=('2021-10-13', None),
    config_path=CONFIG
)
agnostic_entity.run_agnostic_query()
"""    

cross_version_structured_query_p_o = """
query = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
SELECT DISTINCT ?s
WHERE {
    ?s datacite:usesIdentifierScheme datacite:orcid.
}
'''
agnostic_query = VersionQuery(query, config_path=CONFIG)
agnostic_query.run_agnostic_query()
"""

single_version_structured_query_p_o = """
query = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
SELECT DISTINCT ?s
WHERE {
    ?s datacite:usesIdentifierScheme datacite:orcid.
}
'''
agnostic_query = VersionQuery(query, ('2021-10-13', None), config_path=CONFIG)
agnostic_query.run_agnostic_query()
"""

single_delta_structured_query_p_o = """
query = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
SELECT DISTINCT ?s
WHERE {
    ?s datacite:usesIdentifierScheme datacite:orcid.
}
'''
agnostic_entity = DeltaQuery(
    query=query,
    on_time=('2021-10-13', None),
    config_path=CONFIG
)
agnostic_entity.run_agnostic_query()
"""

cross_delta_structured_query_p_o = """
query = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
SELECT DISTINCT ?s
WHERE {
    ?s datacite:usesIdentifierScheme datacite:orcid.
}
'''
agnostic_entity = DeltaQuery(
    query=query,
    config_path=CONFIG
)
agnostic_entity.run_agnostic_query()
"""

entities = [2, 12935, 90837, 96505, 140957, 21821, 16973, 30948, 32497, 32544, 33243, 99238, 26565, 62320, 18046, 49, 141031, 69559, 32787, 86766]

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

