import os, csv
from setup_and_tests import tests, setup
from subprocess import Popen, CREATE_NEW_CONSOLE, PIPE

output_path = "benchmark/benchmark_memory.csv"

def save(path:str, data:list):
    with open(path, "a+", newline="", encoding="utf8") as f:
        writer = csv.writer(f)
        writer.writerow(data)

if not os.path.isfile(output_path):
    save(output_path, ["test_name", "memory"])

repetitions = 1
imports = """
from time_agnostic_library.agnostic_entity import AgnosticEntity
from time_agnostic_library.agnostic_query import VersionQuery, DeltaQuery
import psutil, os
"""
memory = """
print(psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)
"""
for test_name, test in tests.items():
    file_name = f"{test_name}.py"
    with open(file_name, "w+") as f:
        f.write(imports + test + memory)
    memory_usage = 0.0
    for repetition in range(repetitions):
        exec(setup)
        process = Popen(
            ["python", file_name],
            creationflags=CREATE_NEW_CONSOLE,
            stdout=PIPE
        )
        out, err = process.communicate()
        memory_usage += float(out.decode("utf-8").splitlines()[-1])
    save(output_path, [test_name, str(memory_usage / float(repetitions)) + "MB"])
    os.remove(file_name)
