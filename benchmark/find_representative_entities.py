import argparse
import gzip
import logging
import os
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from multiprocessing import Pool, cpu_count

import rdflib
from rdflib import Dataset, Literal, URIRef
from rdflib.namespace import PROV
from SPARQLWrapper import JSON, POST, SPARQLExceptions, SPARQLWrapper
from tqdm import tqdm

OCO = rdflib.Namespace("https://w3id.org/oc/ontology/")

MAX_RETRIES = 3
INITIAL_BACKOFF = 1
TITLE_PROP_URI = "http://purl.org/dc/terms/title"
LITERAL_PROP_URI = "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"

DEFAULT_LOG_LEVEL = logging.INFO
PROPERTIES_TO_TRACK = {TITLE_PROP_URI, LITERAL_PROP_URI}


def _nested_set_defaultdict_factory():
    """Returns a defaultdict suitable for the nested structure."""
    return defaultdict(set)

def execute_sparql_query(endpoint, query, max_retries, initial_backoff):
    """
    Executes a SPARQL query against a specified endpoint using SPARQLWrapper,
    with retry logic and exponential backoff.

    Args:
        endpoint (str): The URL of the SPARQL endpoint.
        query (str): The SPARQL query string.
        max_retries (int): Maximum number of retry attempts.
        initial_backoff (float): Initial backoff time in seconds.

    Returns:
        dict: The parsed JSON response from the endpoint, or None on error.
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    sparql.setMethod(POST)
    sparql.setTimeout(60)

    retries = 0
    backoff = initial_backoff

    while retries < max_retries:
        try:
            results = sparql.query().convert()
            if isinstance(results, dict):
                return results
            else:
                 logging.error(f"SPARQL query result was not a dictionary: {type(results)}")
                 raise TypeError("Unexpected result type from SPARQLWrapper")

        except SPARQLExceptions.QueryBadFormed as e:
            logging.error(f"SPARQL query badly formed: {e}\nQuery:\n{query}")
            return None
        except (urllib.error.URLError, urllib.error.HTTPError, SPARQLExceptions.EndPointNotFound, SPARQLExceptions.EndPointInternalError, TimeoutError, TypeError, Exception) as e:
            retries += 1
            logging.warning(f"SPARQL query failed (Attempt {retries}/{max_retries}): {e}")
            if retries < max_retries:
                logging.info(f"Retrying in {backoff} seconds...")
                time.sleep(backoff)
                backoff *= 2 # Exponential backoff
            else:
                logging.error(f"SPARQL query failed after {max_retries} attempts.")
                return None

# --- Query Definitions ---

# Reference: Inspired by BEAR benchmark's need to test on datasets/entities
# with varying characteristics (e.g., size, update patterns).

# Dimension: History Length (Number of Snapshots)
# Goal: Select entities with short, medium, and long histories.
QUERY_SNAPSHOT_COUNTS = """
PREFIX prov: <http://www.w3.org/ns/prov#>
SELECT ?entity (COUNT(DISTINCT ?snapshot) AS ?snapshotCount)
WHERE {
    GRAPH ?snapshot {
        ?snapshot prov:specializationOf ?entity .
        # Optional filter: Focus on bibliographic resources (br)
        # FILTER STRSTARTS(STR(?entity), "https://w3id.org/oc/meta/br/")
    }
}
GROUP BY ?entity
ORDER BY ASC(?snapshotCount)
"""

# Dimension: Change Frequency/Type (Specific Property Changes)
# Goal: Select entities where specific properties were changed often.
# Example: dcterms:title changes
QUERY_PROPERTY_CHANGE_COUNTS = """
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX oco: <https://w3id.org/oc/ontology/>
SELECT ?entity (COUNT(DISTINCT ?snapshot) AS ?changeCount)
WHERE {
    GRAPH ?snapshot {
        ?snapshot prov:specializationOf ?entity ;
                  # Check if the snapshot has an update query associated
                  oco:hasUpdateQuery ?updateQuery .
        # Filter update queries containing the specific property URI
        FILTER CONTAINS(STR(?updateQuery), "<#PROPERTY_URI#>")
    }
}
GROUP BY ?entity
# Consider only entities where the property was actually modified (count > 1 assumes initial creation + modification)
HAVING (COUNT(DISTINCT ?snapshot) > 1)
ORDER BY DESC(?changeCount)
"""

# --- SPARQL-based Retrieval Functions ---

def get_entities_by_snapshot_count_sparql(endpoint, query=QUERY_SNAPSHOT_COUNTS):
    """Retrieves entity snapshot counts from a SPARQL endpoint."""
    results_data = execute_sparql_query(endpoint, query, MAX_RETRIES, INITIAL_BACKOFF)
    if not results_data or 'results' not in results_data or 'bindings' not in results_data['results']:
        logging.error("Failed to retrieve snapshot counts via SPARQL or data format is incorrect.")
        return None

    bindings = results_data['results']['bindings']
    if not bindings:
        logging.warning("No entities with snapshot counts found via SPARQL.")
        return {}

    entity_counts_dict = {}
    for bind in bindings:
        try:
            entity = bind['entity']['value']
            count = int(bind['snapshotCount']['value'])
            entity_counts_dict[entity] = count
        except (KeyError, ValueError) as e:
            logging.warning(f"Skipping SPARQL binding due to parsing error ({e}): {bind}")
            continue
    return entity_counts_dict # Return {entity_uri: count}

def get_entities_by_frequent_changes_sparql(endpoint, property_uri, query_template=QUERY_PROPERTY_CHANGE_COUNTS):
    """Retrieves entity change counts for a specific property from a SPARQL endpoint."""
    query = query_template.replace("#PROPERTY_URI#", property_uri)
    # The SPARQL query already handles filtering (HAVING > 1) and ordering.
    results_data = execute_sparql_query(endpoint, query, MAX_RETRIES, INITIAL_BACKOFF)
    if not results_data or 'results' not in results_data or 'bindings' not in results_data['results']:
        logging.error(f"Failed to retrieve change counts via SPARQL for {property_uri} or data format is incorrect.")
        return None

    bindings = results_data['results']['bindings']
    if not bindings:
         logging.warning(f"No entities found via SPARQL with > 1 change for property {property_uri}.")
         return {}

    entity_change_counts_dict = {}
    for bind in bindings:
         try:
            entity = bind['entity']['value']
            count = int(bind['changeCount']['value'])
            entity_change_counts_dict[entity] = count
         except (KeyError, ValueError) as e:
            logging.warning(f"Skipping SPARQL binding due to parsing error ({e}): {bind}")
            continue
    return entity_change_counts_dict # Return {entity_uri: count}

def _process_nq_gz_file(file_path):
    """
    Processes a single .nq.gz file to extract snapshot and property change info.

    Args:
        file_path (str): The path to the .nq.gz file.

    Returns:
        tuple: A tuple containing:
            - dict: snapshot_to_entity_map ({snapshot_uri: entity_uri})
            - dict: property_changes_in_file ({property_uri: {entity_uri: {snapshot_uri, ...}}})
            Returns (None, None) if an error occurs during processing.
    """
    snapshot_to_entity_map = {}
    # Use the picklable factory function
    property_changes_in_file = defaultdict(_nested_set_defaultdict_factory)
    temp_graph = Dataset()

    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            temp_graph.parse(source=f, format="nquads")

        # Extract specializationOf triples
        for s, p, o, _ in temp_graph.quads((None, PROV.specializationOf, None)):
            snapshot_uri_str = str(s)
            entity_uri_str = str(o)
            snapshot_to_entity_map[snapshot_uri_str] = entity_uri_str

        # Extract hasUpdateQuery triples and check for property changes
        for s, p, o, _ in temp_graph.quads((None, OCO.hasUpdateQuery, None)):
            snapshot_uri_str = str(s)
            query_str = str(o)

            if snapshot_uri_str in snapshot_to_entity_map:
                entity_uri_str = snapshot_to_entity_map[snapshot_uri_str]
                for prop_uri in PROPERTIES_TO_TRACK:
                    if prop_uri in query_str:
                         property_changes_in_file[prop_uri][entity_uri_str].add(snapshot_uri_str)

        return snapshot_to_entity_map, property_changes_in_file

    except gzip.BadGzipFile:
        logging.error(f"  Error: Bad gzip file format for {os.path.basename(file_path)}. Skipping.")
        return None, None
    except Exception as e:
        logging.error(f"  Error processing file {os.path.basename(file_path)}: {e}. Skipping.")
        return None, None
    finally:
        # Explicitly clear graph to potentially help with memory
        temp_graph = None


def analyze_local_nq_gz_files(directory_path):
    """
    Analyzes .nq.gz files in a directory using multiprocessing to gather snapshot counts
    and property change information without loading all data into memory.

    Args:
        directory_path (str): Path to the directory containing .nq.gz files.

    Returns:
        tuple: A tuple containing:
            - dict: final_entity_snapshot_counts ({entity_uri: count})
            - dict: aggregated_property_changes ({property_uri: {entity_uri: {snapshot_uri, ...}}})
            Returns (None, None) if a critical error occurs (e.g., directory not found or no files processed).
    """
    entity_snapshot_associations = defaultdict(set)
    # Use the picklable factory function here as well for the main aggregation
    aggregated_property_changes = defaultdict(_nested_set_defaultdict_factory)

    logging.info(f"Starting parallel analysis of N-Quads files in: {directory_path}")

    try:
        all_files = [os.path.join(directory_path, f)
                     for f in os.listdir(directory_path)
                     if f.endswith(".nq.gz")]
        total_files = len(all_files)
        logging.info(f"Found {total_files} .nq.gz files to process.")

        if not all_files:
            logging.warning(f"No .nq.gz files found in directory: {directory_path}")
            return {}, {}

    except FileNotFoundError:
        logging.error(f"Error: Directory not found: {directory_path}")
        return None, None
    except Exception as e:
        logging.error(f"An unexpected error occurred while listing directory {directory_path}: {e}")
        return None, None

    num_processes = min(cpu_count(), total_files) if total_files > 0 else 1
    processed_files_count = 0
    logging.info(f"Using {num_processes} processes for analysis.")

    with Pool(processes=num_processes) as pool:
        # Use imap_unordered for potentially better memory usage and progress tracking
        results_iterator = pool.imap_unordered(_process_nq_gz_file, all_files)

        pbar = tqdm(results_iterator, total=total_files, desc="Processing N-Quads files", unit="file")

        for result in pbar:
            if result is not None:
                snapshot_map, prop_changes = result
                if snapshot_map is not None and prop_changes is not None:
                    processed_files_count += 1
                    for snapshot_uri, entity_uri in snapshot_map.items():
                        entity_snapshot_associations[entity_uri].add(snapshot_uri)

                    for prop_uri, entity_changes in prop_changes.items():
                         for entity_uri, snapshots in entity_changes.items():
                             aggregated_property_changes[prop_uri][entity_uri].update(snapshots)

    if processed_files_count == 0:
        if total_files > 0:
            logging.error(f"Found {total_files} .nq.gz files, but none could be processed successfully in: {directory_path}")
        return None, None
    else:
         logging.info(f"Finished parallel analysis. Successfully processed {processed_files_count}/{total_files} files.")

    final_entity_snapshot_counts = {entity: len(snapshots)
                                    for entity, snapshots in entity_snapshot_associations.items()}

    return final_entity_snapshot_counts, aggregated_property_changes


def get_entities_by_snapshot_count(entity_counts_dict, num_samples=5):
    """
    Selects entities based on snapshot counts (min, low-mid, mid, high-mid, max)
    from a pre-calculated dictionary of counts.

    Args:
        entity_counts_dict (dict): Dictionary mapping {entity_uri: snapshot_count}.
        num_samples (int): Target number of samples to retrieve per category.

    Returns:
        dict: A dictionary with keys 'min', 'low_mid', 'mid', 'high_mid', 'max',
              each containing a list of unique entity URIs. Returns empty dict if input is empty/invalid.
    """
    if not entity_counts_dict:
        logging.warning("Entity counts dictionary is empty or invalid.")
        return {}

    # Convert dict to list of dicts for sorting
    entity_counts_list = [{'entity': entity, 'count': count} for entity, count in entity_counts_dict.items()]

    if not entity_counts_list:
        logging.warning("No valid entity counts could be extracted from dictionary.")
        return {}

    entity_counts_list.sort(key=lambda x: x['count'])

    n = len(entity_counts_list)
    if n == 0:
        logging.warning("Entity counts list is empty after processing dictionary.")
        return {}

    indices = {
        'min': 0,
        'low_mid': n // 4,
        'mid': n // 2,
        'high_mid': 3 * n // 4,
        'max': n - 1
    }

    # 1. Initial selection (allows overlaps)
    selected_candidates = defaultdict(list)
    for category, base_index in indices.items():
        count = 0
        safe_base_index = max(0, min(n - 1, base_index))
        start_range = max(0, safe_base_index - num_samples // 2)
        end_range = min(n, safe_base_index + (num_samples + 1) // 2)

        current_candidates = []
        # Attempt 1: Centered selection
        for i in range(start_range, end_range):
            if count < num_samples:
                current_candidates.append(entity_counts_list[i]['entity'])
                count += 1
            else:
                break
        # Attempt 2: Expand outwards
        idx_after = end_range
        idx_before = start_range - 1
        while count < num_samples and (idx_before >= 0 or idx_after < n):
            if idx_after < n:
                candidate = entity_counts_list[idx_after]['entity']
                if candidate not in current_candidates:
                    current_candidates.append(candidate)
                    count += 1
                    if count >= num_samples: break
                idx_after += 1
            if idx_before >= 0:
                candidate = entity_counts_list[idx_before]['entity']
                if candidate not in current_candidates:
                    current_candidates.append(candidate)
                    count += 1
                    if count >= num_samples: break
                idx_before -= 1

        selected_candidates[category] = current_candidates
        logging.info(f"Selected {len(selected_candidates[category])} candidates for category '{category}' (Target index: {base_index}, Approx count: {entity_counts_list[safe_base_index]['count']}) - Uniqueness pending")

    # 2. Ensure uniqueness across categories using priority
    final_selected_entities = defaultdict(list)
    used_entities = set()
    priority_order = ['min', 'max', 'mid', 'low_mid', 'high_mid']

    logging.info("Applying uniqueness constraint to snapshot count categories...")
    for category in priority_order:
        candidates = selected_candidates[category]
        added_count = 0
        for omid in candidates:
            if omid not in used_entities:
                final_selected_entities[category].append(omid)
                used_entities.add(omid)
                added_count += 1
                if added_count >= num_samples:
                    break
        logging.info(f"  Category '{category}': assigned {added_count} unique entities.")
        if added_count < len(candidates) and added_count < num_samples:
             logging.warning(f"  Category '{category}': Fewer than targeted ({num_samples}) unique entities assigned ({added_count}) due to overlaps.")

    return dict(final_selected_entities)

def get_entities_by_frequent_changes(all_property_changes, property_uri, num_samples=5):
    """
    Selects entities where a specific property was frequently changed, based on
    pre-aggregated data.

    Args:
        all_property_changes (dict): Dictionary mapping {property_uri: {entity_uri: {snapshot_uri, ...}}}.
        property_uri (str): The full URI of the property to analyze.
        num_samples (int): Number of top entities to retrieve.

    Returns:
        list: A list of entity URIs for entities with the most changes (>1) for that property.
              Returns empty list on error or if no relevant changes are found.
    """
    if not all_property_changes or property_uri not in all_property_changes:
        logging.warning(f"No change data found for property {property_uri} in the aggregated results.")
        return []

    entity_changes = all_property_changes[property_uri]
    entity_change_counts = defaultdict(int)

    # Calculate counts, filtering for > 1 change
    for entity, snapshots in entity_changes.items():
        count = len(snapshots)
        if count > 1:
            entity_change_counts[entity] = count

    if not entity_change_counts:
         logging.warning(f"No entities found with more than 1 change for property {property_uri}.")
         return []

    # Sort entities by change count (descending) and take top N
    sorted_entities = sorted(entity_change_counts.items(), key=lambda item: item[1], reverse=True)
    selected_entities = [entity for entity, count in sorted_entities[:num_samples]]

    logging.info(f"Selected {len(selected_entities)} entities with most changes (>1) for {property_uri}.")
    return selected_entities

def main():
    """Parses command-line arguments and runs the entity selection process."""
    parser = argparse.ArgumentParser(
        description="Find representative entities from a SPARQL endpoint or local N-Quads (.nq.gz) files based on history length and property changes."
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--endpoint",
        type=str,
        help="URL of the SPARQL provenance endpoint.",
    )
    source_group.add_argument(
        "--local-dir",
        type=str,
        help="Path to the directory containing .nq.gz files.",
    )

    parser.add_argument(
        "--num-samples",
        type=int,
        default=5,
        help="Number of representative entities to select for each category/criterion.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level.",
    )

    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

    snapshot_counts_data = None # Will be {entity: count} or None on error
    property_changes_data = None # Will be {prop: {entity: {snapshot}}} or None on error

    if args.endpoint:
        endpoint = args.endpoint
        print("-" * 60)
        logging.info(f"Using SPARQL Endpoint: {endpoint}")
        print("-" * 60)

        logging.info("Fetching snapshot counts via SPARQL...")
        snapshot_counts_data = get_entities_by_snapshot_count_sparql(endpoint)

        logging.info("Fetching frequent title changes via SPARQL...")
        title_changes_data = get_entities_by_frequent_changes_sparql(endpoint, TITLE_PROP_URI)

        logging.info("Fetching frequent literal changes via SPARQL...")
        literal_changes_data = get_entities_by_frequent_changes_sparql(endpoint, LITERAL_PROP_URI)

        if snapshot_counts_data is None:
             logging.critical("Failed to fetch snapshot counts from SPARQL. Cannot proceed with snapshot analysis.")

    elif args.local_dir:
        print("-" * 60)
        logging.info(f"Using local directory for incremental analysis: {args.local_dir}")
        print("-" * 60)
        snapshot_counts_data, property_changes_data = analyze_local_nq_gz_files(args.local_dir)
        if snapshot_counts_data is None:
            logging.critical("Failed to analyze local files. Exiting.")
            return 

    print("-" * 60)
    logging.info("Processing acquired data to select representative entities...")
    print("-" * 60)

    # 1. Select entities based on snapshot count
    final_snapshot_entities = {}
    if snapshot_counts_data is not None:
        final_snapshot_entities = get_entities_by_snapshot_count(
            snapshot_counts_data,
            num_samples=args.num_samples
        )
    else:
        logging.warning("Skipping snapshot count analysis due to data acquisition errors.")

    print("\nEntities selected by Snapshot Count:")
    if final_snapshot_entities:
        for category, omids in final_snapshot_entities.items():
            print(f"  {category.capitalize()}:")
            for omid in omids:
                print(f"    - {omid}")
    else:
        print("  No entities selected based on snapshot count or error occurred.")
    print("-" * 60)

    # 2. Select entities based on frequent title changes
    final_title_change_entities = []
    if args.endpoint and title_changes_data is not None:
         # SPARQL gives {entity: count}, sort and take top N
         sorted_title_changes = sorted(title_changes_data.items(), key=lambda item: item[1], reverse=True)
         final_title_change_entities = [entity for entity, count in sorted_title_changes[:args.num_samples]]
         logging.info(f"Selected {len(final_title_change_entities)} entities with most title changes (>1) from SPARQL.")
    elif args.local_dir and property_changes_data is not None:
         final_title_change_entities = get_entities_by_frequent_changes(
             property_changes_data,
             TITLE_PROP_URI,
             num_samples=args.num_samples
         )
    else:
         logging.warning("Skipping frequent title change analysis due to data acquisition errors.")

    print("\nEntities selected by Frequent Title Changes:")
    if final_title_change_entities:
        for omid in final_title_change_entities:
            print(f"  - {omid}")
    else:
        print("  No entities selected based on frequent title changes or error occurred.")
    print("-" * 60)

    # 3. Select entities based on frequent literal value changes
    final_literal_change_entities = []
    if args.endpoint and literal_changes_data is not None:
         # SPARQL gives {entity: count}, sort and take top N
         sorted_literal_changes = sorted(literal_changes_data.items(), key=lambda item: item[1], reverse=True)
         final_literal_change_entities = [entity for entity, count in sorted_literal_changes[:args.num_samples]]
         logging.info(f"Selected {len(final_literal_change_entities)} entities with most literal changes (>1) from SPARQL.")
    elif args.local_dir and property_changes_data is not None:
        final_literal_change_entities = get_entities_by_frequent_changes(
            property_changes_data,
            LITERAL_PROP_URI,
            num_samples=args.num_samples
        )
    else:
         logging.warning("Skipping frequent literal value change analysis due to data acquisition errors.")

    print("\nEntities selected by Frequent Literal Value Changes:")
    if final_literal_change_entities:
        for omid in final_literal_change_entities:
            print(f"  - {omid}")
    else:
        print("  No entities selected based on frequent literal value changes or error occurred.")
    print("-" * 60)

    logging.info("Representative entity selection script finished.")


if __name__ == "__main__":
    main()