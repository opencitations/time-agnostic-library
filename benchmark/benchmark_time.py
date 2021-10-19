import timeit
from statistics import median
from setup_and_tests import save, already_done, setup, tests_time_w_out_cache_w_out_index, tests_time_w_out_cache_w_index, tests_time_w_cache_w_out_index, tests_time_w_cache_w_index, create_statistics_file

iterations = 1
repetitions = 10

parameters = {
    "w_cache_w_out_index": tests_time_w_cache_w_out_index,
    "w_out_cache_w_out_index": tests_time_w_out_cache_w_out_index,
    "w_out_cache_w_index": tests_time_w_out_cache_w_index,
    "w_cache_w_index": tests_time_w_cache_w_index
}

create_statistics_file()

for parameter, tests in parameters.items():
    for test_name, test in tests.items():
        if already_done(parameter, test_name, "time"):
            continue
        config_file_name = f"{parameter}.json"
        test = f"CONFIG = '{config_file_name}'\n" + test
        results = list()
        if "w_cache" in parameter:
            setup_empty_cache = setup + f"launch_blazegraph('./db/cache/empty', 29999)\ntime.sleep(10)\nempty_the_cache('{config_file_name}')"
            setup_full_cache = setup + "launch_blazegraph('./db/cache/full', 29999)\ntime.sleep(10)"
            results.extend(timeit.repeat(stmt=test, setup=setup_empty_cache, repeat=1, number=iterations))
            results.extend(timeit.repeat(stmt=test, setup=setup_full_cache, repeat=repetitions-1, number=iterations))
        else:
            setup += "time.sleep(10)"
            results.extend(timeit.repeat(stmt=test, setup=setup, repeat=repetitions, number=iterations))
        out = {
            test_name: {
                "min": min(results),
                "median": median(results),
                "max": max(results)
            }
        }
        save(out, parameter, "time")