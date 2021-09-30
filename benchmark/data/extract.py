# from SPARQLWrapper import SPARQLWrapper, RDFXML
# from time_agnostic_library.sparql import Sparql
# from time_agnostic_library.agnostic_entity import AgnosticEntity
# from time_agnostic_library.agnostic_query import VersionQuery, DeltaQuery
# from pprint import pprint
# import rdflib
from typing import List, Dict, final
from collections import OrderedDict, defaultdict
from csv import DictReader, DictWriter
from statistics import mean, stdev

def complete_data(file_path:str, output_path:str, field:str) -> None:
    input_file = DictReader(open(file_path))
    new_data = list()
    for row in input_file:
        data = [float(datum) for datum in row[field].split("/")]
        row["min"] = min(data)
        row["max"] = max(data)
        row["mean"] = mean(data)
        row["stdev"] = stdev(data)
        new_data.append(row)
    keys = new_data[0].keys()
    with open(output_path, 'w', newline="")  as output_file:
        dict_writer = DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(new_data)

def get_final_data(input_files:dict, output_path:str, result:str) -> None:
    data:List[Dict] = list()
    for input_file, field in input_files.items():
        input_file = DictReader(open(input_file))
        for row in input_file:
            relevant_data = {
                "test_name": row["test_name"],
                f"{field}": row[result]
            }
            existing_key = next((item for item in data if item["test_name"] == row["test_name"]), None)
            if existing_key:
                existing_key.update(relevant_data)
            else:
                data.append(relevant_data)
    keys = ["test_name", "time", "memory", "memory_blazegraph"]
    with open(output_path, 'w', newline="")  as output_file:
        dict_writer = DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)
    

complete_data(
    "benchmark_time_blazegraph.csv", 
    "benchmark_time_blazegraph_statistics.csv",
    "time")

files = OrderedDict({
    "benchmark_time_statistics.csv": "time",
    "benchmark_memory_statistics.csv": "memory",
    "benchmark_memory_blazegraph_statistics.csv": "memory_blazegraph"
})

# get_final_data(
#     files, 
#     "statistics.csv",
#     "mean")





