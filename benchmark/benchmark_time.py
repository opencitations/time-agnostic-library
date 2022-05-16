import timeit
from statistics import median, mean, stdev
from setup_and_tests import \
    save, already_done, get_setup, tests_time_w_out_cache_w_out_index, tests_time_w_out_cache_w_index, \
    tests_time_w_cache_w_out_index, tests_time_w_cache_w_index, create_statistics_file, OUTPUT_PATH, TRIPLESTORES
from triplestore_manager import TriplestoreManager

iterations = 1
repetitions = 10

parameters:dict = {
    'w_cache_w_out_index': tests_time_w_cache_w_out_index,
    'w_out_cache_w_out_index': tests_time_w_out_cache_w_out_index,
    'w_out_cache_w_index': tests_time_w_out_cache_w_index,
    'w_cache_w_index': tests_time_w_cache_w_index
}

for triplestore in TRIPLESTORES:
    output_path = OUTPUT_PATH.replace('.json', f'_{triplestore}.json')
    create_statistics_file(output_path=output_path)
    TriplestoreManager.clear_cache(triplestore)
    for parameter, tests in parameters.items():            
        config_filepath = f'config/{triplestore.lower()}/{parameter}.json'
        setup_no_cache, setup_cache = get_setup(triplestore)
        for test_name, test_entities in tests.items():
            if already_done(parameter, test_name, 'time', output_path=output_path):
                continue
            results = list()
            for test_entity in test_entities:
                test = f"CONFIG = '{config_filepath}'\n" + test_entity
                if 'w_cache' in parameter:
                    times_cache = timeit.repeat(stmt=test, setup=setup_cache, repeat=repetitions, number=iterations)
                    results.extend(times_cache)
                else:
                    times_no_cache = timeit.repeat(stmt=test, setup=setup_no_cache, repeat=repetitions, number=iterations)
                    results.extend(times_no_cache)
            out = {
                'min': min(results),
                'median': median(results),
                'mean': mean(results),
                'stdev': stdev(results),
                'max': max(results)
            }
            save(data=out, test_name=test_name, parameter=parameter, benchmark='time', output_path=output_path)