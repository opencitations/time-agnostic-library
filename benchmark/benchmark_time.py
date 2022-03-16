import timeit
from statistics import median, mean, stdev
from setup_and_tests import save, already_done, setup, tests_time_w_out_cache_w_out_index, tests_time_w_out_cache_w_index, tests_time_w_cache_w_out_index, tests_time_w_cache_w_index, create_statistics_file


iterations = 1
repetitions = 10

parameters:dict = {
    'w_cache_w_out_index': tests_time_w_cache_w_out_index,
    'w_out_cache_w_out_index': tests_time_w_out_cache_w_out_index,
    'w_out_cache_w_index': tests_time_w_out_cache_w_index,
    'w_cache_w_index': tests_time_w_cache_w_index
}

create_statistics_file()

for parameter, tests in parameters.items():
    config_file_name = f'{parameter}.json'
    setup_empty_cache = setup + f"launch_blazegraph('./db/cache/empty', 29999)\ntime.sleep(10)\nempty_the_cache('{config_file_name}')"
    setup_full_cache = setup + "launch_blazegraph('./db/cache/full', 29999)\ntime.sleep(10)"
    setup_no_cache = setup + 'time.sleep(10)'
    for test_name, test_entities in tests.items():
        if already_done(parameter, test_name, 'time'):
            continue
        results = list()
        for test_entity in test_entities:
            test = f"CONFIG = '{config_file_name}'\n" + test_entity
            if 'w_cache' in parameter:
                results.extend(timeit.repeat(stmt=test, setup=setup_empty_cache, repeat=1, number=iterations))
                results.extend(timeit.repeat(stmt=test, setup=setup_full_cache, repeat=repetitions-1, number=iterations))
            else:
                results.extend(timeit.repeat(stmt=test, setup=setup_no_cache, repeat=repetitions, number=iterations))
        out = {
            test_name: {
                'min': min(results),
                'median': median(results),
                'mean': mean(results),
                'stdev': stdev(results),
                'max': max(results)
            }
        }
        save(out, parameter, 'time')