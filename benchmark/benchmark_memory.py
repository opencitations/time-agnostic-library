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


from setup_and_tests import save, already_done, get_setup, tests_memory_w_cache_w_out_index, \
    tests_memory_w_out_cache_w_out_index, create_statistics_file, save_baseline, \
    OUTPUT_PATH, TRIPLESTORES
from subprocess import Popen, PIPE
from sys import platform
from triplestore_manager import TriplestoreManager
IS_UNIX = platform == 'darwin' or platform.startswith('linux')
if not IS_UNIX:
    from subprocess import CREATE_NEW_CONSOLE
import os, re


repetitions = 10
parameters = {
    'w_cache_w_out_index': tests_memory_w_cache_w_out_index,
    'w_out_cache_w_out_index': tests_memory_w_out_cache_w_out_index
}


def benchmark_memory(parameter:str, tests:dict, repetitions:int):
    python = 'python3' if IS_UNIX else 'python'
    for triplestore in TRIPLESTORES: 
        # output_path = OUTPUT_PATH.replace('.json', f'_{triplestore}.json')
        # create_statistics_file(output_path=output_path)
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
        if 'w_cache' in parameter:
            conditional_setup = setup_cache
        else:
            conditional_setup = setup_no_cache
        for test_name, test_entities in tests.items():
            for test_entity in test_entities:
                entity_match = re.search('<https://github.com/opencitations/time-agnostic-library/br/(\d+)>', test_entity)
                entity = f':br/{entity_match.group(1)}' if entity_match else 'p_o'
                work_done = already_done(parameter, test_name, 'memory', entity, repetitions, output_path=output_path)
                if work_done == repetitions:
                    continue
                repetitions_to_do = repetitions - work_done
                file_name = f'{test_name}.py'
                with open(file_name, 'w+') as f:
                    f.write(imports + test_entity + memory)
                if 'w_cache' in parameter and repetitions_to_do == repetitions:
                    TriplestoreManager.clear_cache(triplestore)
                elif not 'w_cache' in parameter:
                    save_baseline(imports + setup_no_cache, test_entity, 'memory', test_name, entity, output_path, config_filepath)
                for _ in range(repetitions_to_do):
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
                    memory_usage = float(out.decode('utf-8').splitlines()[-1])
                    save(data=memory_usage, test_name=test_name, parameter=parameter, benchmark='memory', entity=entity, output_path=output_path)
                os.remove(file_name)

for parameter, tests in parameters.items():
    benchmark_memory(parameter, tests, repetitions)

