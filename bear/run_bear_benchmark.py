import json
import os
import time

from time_agnostic_library.agnostic_query import \
    VersionQuery  # Assumendo che la libreria sia così importabile


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

if __name__ == "__main__":
    BASE_FOLDER = 'C:/Users/arcangelo.massari2/Desktop/time-agnostic-library'

    with open(os.path.join(BASE_FOLDER, "timestamps.json"), "r") as file:
        timestamps = json.load(file)

    # Lista di query SPARQL
    query_folder = os.path.join(BASE_FOLDER, "bear", "queries", "bear_a")
    all_results = {}

    start_time = None  # Puoi specificare un inizio se necessario
    end_time = None  # Puoi specificare una fine se necessario
    config_path = os.path.join(BASE_FOLDER, "bear", "config.json")

    for file_name in os.listdir(query_folder):
        if file_name.endswith('.txt'):
            file_path = os.path.join(query_folder, file_name)
            for key, timestamp in timestamps.items():
                results = run_benchmark(file_path, timestamp, key, config_path)
                all_results.update(results)

    with open(os.path.join(BASE_FOLDER, "bear", "bear_a_results.json"), "w") as outfile:
        json.dump(all_results, outfile, indent=4)
