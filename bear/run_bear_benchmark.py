import argparse
import json
import os
import time

from time_agnostic_library.agnostic_query import VersionQuery


def run_benchmark(query_file_path, timestamp, key, config_path):
    with open(query_file_path, 'r') as file:
        queries = file.readlines()

    # Dizionario per conservare i risultati
    results = {}

    for query_string in queries:
        formatted_query = format_query(query_string)
        agnostic_query = VersionQuery(query=formatted_query, on_time=(timestamp, timestamp), config_path=config_path)
        
        # Misura il tempo di inizio
        start = time.time()
        
        # Esegui la query
        query_result = agnostic_query.run_agnostic_query()
        
        # Misura il tempo di fine
        end = time.time()

        # Salva il tempo impiegato e il risultato
        results[query_string] = {
            'filename': os.path.splitext(os.path.basename(query_file_path))[0],  # Nome del file senza estensione
            'timestamp_key': key,
            'timestamp_value': timestamp,
            'duration': end - start,
            'result': query_result
        }
        print(results)
    return results

def format_query(raw_query):
    return f"SELECT * WHERE {{ {raw_query.strip()} }}"

def main(query_folder, timestamp_path, config_path, result_path):
    with open(timestamp_path, "r") as file:
        timestamps = json.load(file)

    all_results = {}

    for file_name in os.listdir(query_folder):
        if file_name.endswith('.txt'):
            file_path = os.path.join(query_folder, file_name)
            for key, timestamp in timestamps.items():
                results = run_benchmark(file_path, timestamp, key, config_path)
                all_results.update(results)

    with open(result_path, "w") as outfile:
        json.dump(all_results, outfile, indent=4)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a SPARQL benchmark test")
    parser.add_argument('query_folder', type=str, help="Folder containing SPARQL query files")
    parser.add_argument('timestamp_path', type=str, help="Path to the JSON file with timestamps")
    parser.add_argument('config_path', type=str, help="Path to the configuration JSON file")
    parser.add_argument('result_path', type=str, help="Path where to save the results JSON file")
    
    args = parser.parse_args()
    main(args.query_folder, args.timestamp_path, args.config_path, args.result_path)

# python3 -m bear.run_bear_benchmark /srv/data/arcangelo/bear/b/queries/ /srv/data/arcangelo/bear/b/instant_ocdm/cache.json /srv/data/arcangelo/bear/b/instant_ocdm/config.json /srv/data/arcangelo/bear/b/instant_ocdm/results.json