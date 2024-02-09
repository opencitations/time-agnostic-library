import argparse
import json
from SPARQLWrapper import SPARQLWrapper, JSON


def execute_sparql_query(endpoint_url):
    """
    Esegue una query SPARQL all'endpoint specificato utilizzando SPARQLWrapper e restituisce i timestamp ordinati.
    """
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setQuery("""
        PREFIX prov: <http://www.w3.org/ns/prov#>
        SELECT DISTINCT ?timestamp
        WHERE {
            ?se a prov:Entity;
                prov:generatedAtTime ?timestamp
        }
        ORDER BY ?timestamp
    """)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()

    timestamps = [result['timestamp']['value'] for result in results['results']['bindings']]
    return timestamps

def main(endpoint_url, output_file):
    """
    Interroga l'endpoint SPARQL specificato per ottenere i timestamp e li salva in un file JSON.
    """
    timestamps = execute_sparql_query(endpoint_url)
    with open(output_file, "w", encoding='utf-8') as json_file:
        json.dump({str(i): ts for i, ts in enumerate(timestamps)}, json_file, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find prov:generatedAtTime objects in .nq files.")
    parser.add_argument("endpoint_url", type=str, help="The SPARQL endpoint URL")
    parser.add_argument("--output", type=str, default="timestamps.json", help="Output file path (default: timestamps.json)")
    args = parser.parse_args()
    main(args.endpoint_url, args.output)