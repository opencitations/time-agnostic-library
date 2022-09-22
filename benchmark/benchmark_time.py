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


from setup_and_tests import \
    save, already_done, get_setup, tests_time_w_out_cache_w_out_index, tests_time_w_out_cache_w_index, \
    tests_time_w_cache_w_out_index, tests_time_w_cache_w_index, create_statistics_file, save_baseline, \
    OUTPUT_PATH, TRIPLESTORES
from triplestore_manager import TriplestoreManager
import re
import timeit


iterations = 1
repetitions = 10
parameters:dict = {
    'w_cache_w_out_index': tests_time_w_cache_w_out_index,
    # 'w_out_cache_w_out_index': tests_time_w_out_cache_w_out_index,
    # 'w_out_cache_w_index': tests_time_w_out_cache_w_index,
    # 'w_cache_w_index': tests_time_w_cache_w_index
}


for triplestore in TRIPLESTORES:
    # output_path = OUTPUT_PATH.replace('.json', f'_{triplestore}.json')
    # create_statistics_file(output_path=output_path)
    for parameter, tests in parameters.items():            
        config_filepath = f'config/{triplestore.lower()}/{parameter}.json'
        setup_no_cache, setup_cache = get_setup(triplestore)
        for test_name, test_entities in tests.items():
            for test_entity in test_entities:
                entity_match = re.search('https://github.com/opencitations/time-agnostic-library/br/(\d+)', test_entity)
                entity = f':br/{entity_match.group(1)}' if entity_match else 'p_o'
                # work_done = already_done(parameter, test_name, 'time', entity, repetitions, output_path=output_path)
                # if work_done == repetitions:
                #     continue
                # repetitions_to_do = repetitions - work_done
                test = f"CONFIG = '{config_filepath}'\n" + test_entity
                # if 'w_cache' in parameter:
                #     if repetitions_to_do == repetitions:
                #         TriplestoreManager.clear_cache(triplestore)
                #     setup_to_use = setup_cache
                # else:
                #     setup_to_use = setup_no_cache
                #     save_baseline(setup_to_use, test, 'time', test_name, entity, output_path, config_filepath)
                for _ in range(1):
                    duration = timeit.timeit(stmt=test, setup=setup_cache, number=1)
                    # save(data=duration, test_name=test_name, parameter=parameter, benchmark='time', entity=entity, output_path=output_path)
