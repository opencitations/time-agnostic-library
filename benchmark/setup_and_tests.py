import json, os

output_path = "statistics.json"

def create_statistics_file():
    if not os.path.isfile(output_path):
        with open(output_path, 'w') as f:
            json.dump({
                "time": {
                    "w_out_cache_w_out_index": dict(),
                    "w_out_cache_w_index": dict(),
                    "w_cache_w_out_index": dict(),
                    "w_cache_w_index": dict()
                },
                "RAM": {
                    "w_cache_w_out_index": dict(), 
                    "w_out_cache_w_out_index": dict()
                }}, f, indent=4)

def save(data:list, parameter:str, benchmark:str): 
    with open(output_path, 'r', encoding="utf-8") as reader:
        prev_data = json.load(reader)
    with open(output_path, 'w', encoding="utf-8") as writer:
        prev_data[benchmark][parameter].update(data)
        json.dump(prev_data, writer, indent=4)

def already_done(parameter:str, test_name:str, benchmark:str) -> bool:
    already_done = False
    with open(output_path, 'r', encoding="utf-8") as reader:
        prev_data = json.load(reader)
        if test_name in prev_data[benchmark][parameter]:
            already_done = True
            print(f"Benchmark on {benchmark} completed: {test_name} - {parameter}")
    return already_done

setup = """
from time_agnostic_library.agnostic_entity import AgnosticEntity
from time_agnostic_library.agnostic_query import VersionQuery, DeltaQuery
from time_agnostic_library.support import empty_the_cache
import os, signal, psutil, time
from subprocess import Popen, CREATE_NEW_CONSOLE

def find_pid_by_name(processName):
    listOfProcessObjects = []
    for proc in psutil.process_iter():
       try:
           pinfo = proc.as_dict(attrs=["pid", "name"])
           # Check if process name contains the given name string.
           if processName.lower() in pinfo["name"].lower() :
               listOfProcessObjects.append(pinfo["pid"])
       except (psutil.NoSuchProcess, psutil.AccessDenied , psutil.ZombieProcess):
           pass
    return listOfProcessObjects

def close_program_by_name(name):
    pids = find_pid_by_name(name)
    for pid in pids:
        try:
            # Linux
            os.kill(pid, signal.SIGSTOP)
        except AttributeError:
            # Windows
            os.kill(pid, signal.SIGTERM)

def launch_blazegraph(ts_dir:str, port:int):
    Popen(
        ["java", "-server", "-Xmx4g", F"-Dcom.bigdata.journal.AbstractJournal.file={ts_dir}/blazegraph.jnl",f"-Djetty.port={port}", "-jar", f"{ts_dir}/blazegraph.jar"],
        creationflags=CREATE_NEW_CONSOLE
    )

close_program_by_name("java")
launch_blazegraph("./db", 9999)
launch_blazegraph("./db/prov", 19999)
"""

def generate_tests_for_entities(test:str, entities:list) -> list:
    tests_for_entities = list()
    for entity in entities:
        tests_for_entities.append(test.replace('{entity}', str(entity)))
    return tests_for_entities

materialization_all_versions = """
agnostic_entity = AgnosticEntity(res="https://github.com/opencitations/time-agnostic-library/br/{entity}", config_path=CONFIG)
agnostic_entity.get_history(include_prov_metadata=True)
"""
materialization_single_version = """
agnostic_entity = AgnosticEntity(res="https://github.com/opencitations/time-agnostic-library/br/{entity}", config_path=CONFIG)
agnostic_entity.get_state_at_time(time=("2021-10-13", None), include_prov_metadata=True)
"""
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
agnostic_query = VersionQuery(query, ("2021-10-13", None), config_path=CONFIG)
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
    on_time=("2021-10-13", None),
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
agnostic_query = VersionQuery(query, ("2021-10-13", None), config_path=CONFIG)
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
    on_time=("2021-10-13", None),
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
    "materialization_all_versions": generate_tests_for_entities(test=materialization_all_versions, entities=entities),
    "materialization_single_version": generate_tests_for_entities(test=materialization_single_version, entities=entities),
    "cross_version_structured_query": generate_tests_for_entities(test=cross_version_structured_query, entities=entities),
    "single_version_structured_query": generate_tests_for_entities(test=single_version_structured_query, entities=entities),
    "cross_version_structured_query_p_o": [cross_version_structured_query_p_o],
    "single_version_structured_query_p_o": [single_version_structured_query_p_o],
    "cross_delta_structured_query": generate_tests_for_entities(test=cross_delta_structured_query, entities=entities),
    "single_delta_structured_query": generate_tests_for_entities(test=single_delta_structured_query, entities=entities),
    "cross_delta_structured_query_p_o": [cross_delta_structured_query_p_o],
    "single_delta_structured_query_p_o": [single_delta_structured_query_p_o]
}

tests_time_w_out_cache_w_index = {
    "cross_version_structured_query_p_o": [cross_version_structured_query_p_o],
    "single_version_structured_query_p_o": [single_version_structured_query_p_o],
    "cross_delta_structured_query_p_o": [cross_delta_structured_query_p_o],
    "single_delta_structured_query_p_o": [single_delta_structured_query_p_o]
}

tests_time_w_cache_w_out_index = {
    "cross_version_structured_query": generate_tests_for_entities(test=cross_version_structured_query, entities=entities),
    "single_version_structured_query": generate_tests_for_entities(test=single_version_structured_query, entities=entities),
    "cross_version_structured_query_p_o": [cross_version_structured_query_p_o],
    "single_version_structured_query_p_o": [single_version_structured_query_p_o]
}

tests_time_w_cache_w_index = {
    "cross_version_structured_query_p_o": [cross_version_structured_query_p_o],
    "single_version_structured_query_p_o": [single_version_structured_query_p_o]
}

tests_memory_w_out_cache_w_out_index = {
    "materialization_all_versions": generate_tests_for_entities(test=materialization_all_versions, entities=entities),
    "materialization_single_version": generate_tests_for_entities(test=materialization_single_version, entities=entities),
    "cross_version_structured_query": generate_tests_for_entities(test=cross_version_structured_query, entities=entities),
    "single_version_structured_query": generate_tests_for_entities(test=single_version_structured_query, entities=entities),
    "cross_version_structured_query_p_o": [cross_version_structured_query_p_o],
    "single_version_structured_query_p_o": [single_version_structured_query_p_o],
    "cross_delta_structured_query": generate_tests_for_entities(test=cross_delta_structured_query, entities=entities),
    "single_delta_structured_query": generate_tests_for_entities(test=single_delta_structured_query, entities=entities),
    "cross_delta_structured_query_p_o": [cross_delta_structured_query_p_o],
    "single_delta_structured_query_p_o": [single_delta_structured_query_p_o]
}

tests_memory_w_cache_w_out_index = {
    "cross_version_structured_query": generate_tests_for_entities(test=cross_version_structured_query, entities=entities),
    "single_version_structured_query": generate_tests_for_entities(test=single_version_structured_query, entities=entities),
    "cross_version_structured_query_p_o": [cross_version_structured_query_p_o],
    "single_version_structured_query_p_o": [single_version_structured_query_p_o]
}

