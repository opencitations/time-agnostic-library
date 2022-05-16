import os
from setup_and_tests import save, already_done, get_setup, tests_memory_w_cache_w_out_index, \
    tests_memory_w_out_cache_w_out_index, create_statistics_file, OUTPUT_PATH, TRIPLESTORES
from subprocess import Popen, PIPE
from statistics import median, mean, stdev
from sys import platform
from triplestore_manager import TriplestoreManager
IS_UNIX = platform == 'darwin' or platform.startswith('linux')
if not IS_UNIX:
    from subprocess import CREATE_NEW_CONSOLE

parameters = {
    'w_cache_w_out_index': tests_memory_w_cache_w_out_index,
    'w_out_cache_w_out_index': tests_memory_w_out_cache_w_out_index
}

def benchmark_memory(parameter:str, tests:dict):
    python = 'python3' if IS_UNIX else 'python'
    repetitions = 10
    for triplestore in TRIPLESTORES: 
        output_path = OUTPUT_PATH.replace('.json', f'_{triplestore}.json')
        create_statistics_file(output_path=output_path)
        config_filepath = f'config/{triplestore.lower()}/{parameter}.json'
        imports = f"""
from time_agnostic_library.agnostic_entity import AgnosticEntity
from time_agnostic_library.agnostic_query import VersionQuery, DeltaQuery
import psutil, os
CONFIG = '{config_filepath}'
        """
        memory = '''
print(psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)
        '''
        setup_no_cache, setup_cache = get_setup(triplestore)
        for test_name, test_entities in tests.items():
            if already_done(parameter, test_name, 'memory', output_path=output_path):
                continue
            memory_usage = list()
            for test_entity in test_entities:
                file_name = f'{test_name}.py'
                with open(file_name, 'w+') as f:
                    f.write(imports + test_entity + memory)
                results_for_this_entity = list()
                for _ in range(repetitions):
                    if 'w_cache' in parameter:
                        conditional_setup = setup_cache
                    else:
                        conditional_setup = setup_no_cache
                    exec(conditional_setup, globals())
                    if IS_UNIX:
                        process = Popen(
                            [python, file_name],
                            stdout=PIPE,
                            stderr=PIPE
                        )
                    else:
                        process = Popen(
                            [python, file_name],
                            creationflags=CREATE_NEW_CONSOLE,
                            stdout=PIPE
                        )
                    out, _ = process.communicate()
                    results_for_this_entity.append(float(out.decode('utf-8').splitlines()[-1]))
                memory_usage.extend(results_for_this_entity)
                os.remove(file_name)
            output = {
                test_name: {
                    'min': min(memory_usage),
                    'median': median(memory_usage),
                    'mean': mean(memory_usage),
                    'stdev': stdev(memory_usage),
                    'max': max(memory_usage)
                }
            }
            save(data=output, test_name=test_name, parameter=parameter, benchmark='memory', output_path=output_path)

for triplestore in TRIPLESTORES:
    TriplestoreManager.clear_cache(triplestore)           

for parameter, tests in parameters.items():
    benchmark_memory(parameter, tests)

