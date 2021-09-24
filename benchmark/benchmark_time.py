import timeit, csv

setup = """
from time_agnostic_library.agnostic_entity import AgnosticEntity
from time_agnostic_library.agnostic_query import VersionQuery, DeltaQuery
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
launch_blazegraph("C:/Users/arcan/OneDrive - Alma Mater Studiorum Università di Bologna/Desktop/time_agnostic/db/final2/old", 9999)
launch_blazegraph("C:/Users/arcan/OneDrive - Alma Mater Studiorum Università di Bologna/Desktop/time_agnostic/db/final2_prov/old", 19999)
time.sleep(10)
"""
materialization_single_version = """
agnostic_entity = AgnosticEntity(res="https://github.com/arcangelo7/time_agnostic/id/61956", config_path="./config.json")
agnostic_entity.get_state_at_time(time=("2021-09-13", None), include_prov_metadata=True)
"""
materialization_all_versions = """
agnostic_entity = AgnosticEntity(res="https://github.com/arcangelo7/time_agnostic/id/61956", config_path="./config.json")
agnostic_entity.get_history(include_prov_metadata=True)
"""
cross_version_structured_query = """
query = '''
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX cito: <http://purl.org/spar/cito/>
PREFIX datacite: <http://purl.org/spar/datacite/> 
SELECT DISTINCT ?br ?id ?value
WHERE {
    <https://github.com/arcangelo7/time_agnostic/br/69211> cito:cites ?br.
    ?br datacite:hasIdentifier ?id.
    OPTIONAL {?id literal:hasLiteralValue ?value.}
}   
'''
agnostic_query = VersionQuery(query, config_path="./config.json")
agnostic_query.run_agnostic_query()
"""
single_version_structured_query = """
query = '''
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX cito: <http://purl.org/spar/cito/>
PREFIX datacite: <http://purl.org/spar/datacite/> 
SELECT DISTINCT ?br ?id ?value
WHERE {
    <https://github.com/arcangelo7/time_agnostic/br/69211> cito:cites ?br.
    ?br datacite:hasIdentifier ?id.
    OPTIONAL {?id literal:hasLiteralValue ?value.}
}   
'''
agnostic_query = VersionQuery(query, ("2021-09-09", "2021-09-14"), config_path="./config.json")
agnostic_query.run_agnostic_query()
"""
cross_version_structured_query_multi_cont_values = """
query = '''
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX cito: <http://purl.org/spar/cito/>
PREFIX datacite: <http://purl.org/spar/datacite/>
SELECT DISTINCT ?br ?id ?value
WHERE {
    <https://github.com/arcangelo7/time_agnostic/br/2> cito:cites ?br.
    ?br datacite:hasIdentifier ?id.
    ?id literal:hasLiteralValue ?value.
}   
'''
agnostic_query = VersionQuery(query, config_path="./config.json")
agnostic_query.run_agnostic_query()
"""
cross_version_structured_query_p_o = """
query = '''
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX pro: <http://purl.org/spar/pro/>
SELECT DISTINCT ?a ?id
WHERE {
    <https://github.com/arcangelo7/time_agnostic/ra/4> datacite:hasIdentifier ?id.
    ?id literal:hasLiteralValue ?literalValue.
    OPTIONAL {?a pro:isHeldBy <https://github.com/arcangelo7/time_agnostic/ra/15519>.}
}
'''
agnostic_query = VersionQuery(query, config_path="./config.json")
agnostic_query.run_agnostic_query()
"""
single_version_structured_query_p_o = """
query = '''
PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
PREFIX datacite: <http://purl.org/spar/datacite/>
PREFIX pro: <http://purl.org/spar/pro/>
SELECT DISTINCT ?a ?id
WHERE {
    <https://github.com/arcangelo7/time_agnostic/ra/4> datacite:hasIdentifier ?id.
    ?id literal:hasLiteralValue ?literalValue.
    OPTIONAL {?a pro:isHeldBy <https://github.com/arcangelo7/time_agnostic/ra/15519>.}
} 
'''
agnostic_query = VersionQuery(query, ("2021-09-09", "2021-09-14"), config_path="./config.json")
agnostic_query.run_agnostic_query()
"""
cross_delta_structured_query = """
query = '''
PREFIX cito: <http://purl.org/spar/cito/>
SELECT DISTINCT ?id
WHERE {
    <https://github.com/arcangelo7/time_agnostic/br/69211> cito:cites ?br.
}
'''
agnostic_entity = DeltaQuery(
    query=query, 
    config_path="./config.json"
)
agnostic_entity.run_agnostic_query()
"""
single_delta_structured_query = """
query = '''
PREFIX cito: <http://purl.org/spar/cito/>
SELECT DISTINCT ?id
WHERE {
    <https://github.com/arcangelo7/time_agnostic/br/69211> cito:cites ?br.
}
'''
agnostic_entity = DeltaQuery(
    query=query, 
    on_time=("2021-09-09", "2021-09-14"),
    config_path="./config.json"
)
agnostic_entity.run_agnostic_query()
"""
single_delta_structured_query_p_o = """
query = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
SELECT DISTINCT ?id
WHERE {
    ?id a datacite:Identifier. 
}
'''
agnostic_entity = DeltaQuery(
    query=query, 
    on_time=("2021-09-09", "2021-09-14"),
    changed_properties={"http://www.essepuntato.it/2010/06/literalreification/"}, 
    config_path="./config.json"
)
agnostic_entity.run_agnostic_query()
"""
cross_delta_structured_query_p_o = """
query = '''
PREFIX datacite: <http://purl.org/spar/datacite/>
SELECT DISTINCT ?id
WHERE {
    ?id a datacite:Identifier. 
}
'''
agnostic_entity = DeltaQuery(
    query=query, 
    changed_properties={"http://www.essepuntato.it/2010/06/literalreification/"}, 
    config_path="./config.json"
)
agnostic_entity.run_agnostic_query()
"""

output_path = "benchmark/benchmark_time.csv"
iterations = 1
repetitions = 10
tests = {
    "materialization_all_versions": materialization_all_versions,
    "materialization_single_version": materialization_single_version,
    "cross_version_structured_query": cross_version_structured_query,
    "single_version_structured_query": single_version_structured_query,
    "cross_version_structured_query_multi_cont_values": cross_version_structured_query_multi_cont_values,
    "cross_version_structured_query_p_o": cross_version_structured_query_p_o,
    "single_version_structured_query_p_o": single_version_structured_query_p_o,
    "cross_delta_structured_query": cross_delta_structured_query,
    "single_delta_structured_query": single_delta_structured_query,
    "single_delta_structured_query_p_o": single_delta_structured_query_p_o,
    "cross_delta_structured_query_p_o": cross_delta_structured_query_p_o
}

def save(path:str, data:list):
    with open(path, "a+", newline="", encoding="utf8") as f:
        writer = csv.writer(f)
        writer.writerow(data)

save(output_path, ["test_name", "time"])
for test_name, test in tests.items():
    results = timeit.repeat(stmt=test, setup=setup, repeat=repetitions, number=iterations)
    save(output_path, [test_name, sum(results)/repetitions])
