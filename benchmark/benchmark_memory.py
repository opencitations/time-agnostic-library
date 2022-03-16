import os
import time
from setup_and_tests import save, already_done, setup, tests_memory_w_cache_w_out_index, tests_memory_w_out_cache_w_out_index, create_statistics_file
from subprocess import Popen, CREATE_NEW_CONSOLE, PIPE
from statistics import median, mean, stdev

parameters = {
    'w_out_cache_w_out_index': tests_memory_w_out_cache_w_out_index,
    'w_cache_w_out_index': tests_memory_w_cache_w_out_index,
}

def benchmark_memory(parameter:str, tests:dict, setup:str):
    repetitions = 10
    config_file_name = f'{parameter}.json'
    imports = f"""
from time_agnostic_library.agnostic_entity import AgnosticEntity
from time_agnostic_library.agnostic_query import VersionQuery, DeltaQuery
import psutil, os
CONFIG = '{config_file_name}'
    """
    memory = '''
print(psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)
    '''
    for test_name, test_entities in tests.items():
        if already_done(parameter, test_name, 'RAM'):
            continue
        memory_usage = list()
        for test_entity in test_entities:
            file_name = f'{test_name}.py'
            with open(file_name, 'w+') as f:
                f.write(imports + test_entity + memory)
            for i in range(repetitions):
                conditional_setup = setup
                if 'w_cache' in parameter:
                    if i == 0:
                        conditional_setup = setup + f"launch_blazegraph('./db/cache/empty', 29999)\ntime.sleep(10)\nempty_the_cache('{config_file_name}')"
                    elif i > 0:
                        conditional_setup = setup + "launch_blazegraph('./db/cache/full', 29999)\ntime.sleep(10)"
                else:
                    conditional_setup = setup + '\ntime.sleep(10)'
                exec(conditional_setup, globals())
                process = Popen(
                    ['python', file_name],
                    creationflags=CREATE_NEW_CONSOLE,
                    stdout=PIPE
                )
                out, _ = process.communicate()
                memory_usage.append(float(out.decode('utf-8').splitlines()[-1]))
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
        save(output, parameter, 'RAM')
        
create_statistics_file()

for parameter, tests in parameters.items():
    benchmark_memory(parameter, tests, setup)

