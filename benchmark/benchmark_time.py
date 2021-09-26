import timeit, csv, os
from setup_and_tests import tests, setup

output_path = "benchmark/benchmark_time.csv"
iterations = 1
repetitions = 1

def save(path:str, data:list):
    with open(path, "a+", newline="", encoding="utf8") as f:
        writer = csv.writer(f)
        writer.writerow(data)

if not os.path.isfile(output_path):
    save(output_path, ["test_name", "time"])
for test_name, test in tests.items():
    results = timeit.repeat(stmt=test, setup=setup, repeat=repetitions, number=iterations)
    save(output_path, [test_name, sum(results)/repetitions])
