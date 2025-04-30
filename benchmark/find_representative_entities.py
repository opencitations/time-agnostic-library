import logging
import time
import urllib.parse
import urllib.request
from collections import defaultdict

from SPARQLWrapper import JSON, POST, SPARQLExceptions, SPARQLWrapper

# --- Configuration ---
# !!! IMPORTANT: Replace with your actual SPARQL endpoint URL !!!
PROVENANCE_SPARQL_ENDPOINT = "http://localhost:8891/sparql"
MAX_RETRIES = 3
INITIAL_BACKOFF = 1 # seconds

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def execute_sparql_query(endpoint, query):
    """
    Executes a SPARQL query against a specified endpoint using SPARQLWrapper,
    with retry logic and exponential backoff.

    Args:
        endpoint (str): The URL of the SPARQL endpoint.
        query (str): The SPARQL query string.

    Returns:
        dict: The parsed JSON response from the endpoint, or None on error.
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    sparql.setMethod(POST)
    sparql.setTimeout(60)

    retries = 0
    backoff = INITIAL_BACKOFF

    while retries < MAX_RETRIES:
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
            logging.warning(f"SPARQL query failed (Attempt {retries}/{MAX_RETRIES}): {e}")
            if retries < MAX_RETRIES:
                logging.info(f"Retrying in {backoff} seconds...")
                time.sleep(backoff)
                backoff *= 2 # Exponential backoff
            else:
                logging.error(f"SPARQL query failed after {MAX_RETRIES} attempts.")
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


def get_entities_by_snapshot_count(endpoint, query=QUERY_SNAPSHOT_COUNTS, num_samples=5):
    """
    Finds entities based on snapshot counts (min, low-mid, mid, high-mid, max),
    ensuring each selected entity is unique across categories.

    Args:
        endpoint (str): SPARQL endpoint URL.
        query (str): The SPARQL query to execute (should return ?entity ?snapshotCount).
        num_samples (int): Target number of samples to retrieve per category.

    Returns:
        dict: A dictionary with keys 'min', 'low_mid', 'mid', 'high_mid', 'max',
              each containing a list of unique OMID strings. Returns empty dict on error.
    """
    logging.info("Finding entities by snapshot count...")
    results_data = execute_sparql_query(endpoint, query)
    if not results_data or 'results' not in results_data or 'bindings' not in results_data['results']:
        logging.error("Failed to retrieve snapshot counts or data format is incorrect.")
        return {}

    bindings = results_data['results']['bindings']
    if not bindings:
        logging.warning("No entities with snapshot counts found.")
        return {}

    entity_counts = []
    for bind in bindings:
        try:
            entity = bind['entity']['value']
            count = int(bind['snapshotCount']['value'])
            entity_counts.append({'entity': entity, 'count': count})
        except (KeyError, ValueError) as e:
            logging.warning(f"Skipping binding due to parsing error ({e}): {bind}")
            continue

    if not entity_counts:
        logging.warning("No valid entity counts could be extracted.")
        return {}

    entity_counts.sort(key=lambda x: x['count'])

    n = len(entity_counts)
    if n == 0:
        logging.warning("Entity counts list is empty after processing bindings.")
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
        # Ensure base_index is within bounds
        safe_base_index = max(0, min(n - 1, base_index))

        # Try to get samples centered around the target index
        start_range = max(0, safe_base_index - num_samples // 2)
        end_range = min(n, safe_base_index + (num_samples + 1) // 2)

        # Attempt 1: Centered selection
        current_candidates = []
        for i in range(start_range, end_range):
            if count < num_samples:
                current_candidates.append(entity_counts[i]['entity'])
                count += 1
            else:
                break

        # Attempt 2: Expand outwards if not enough samples found centrally
        idx_after = end_range
        idx_before = start_range - 1
        while count < num_samples and (idx_before >= 0 or idx_after < n):
            # Expand forward
            if idx_after < n:
                candidate = entity_counts[idx_after]['entity']
                if candidate not in current_candidates:
                    current_candidates.append(candidate)
                    count += 1
                    if count >= num_samples: break
                idx_after += 1
            # Expand backward
            if idx_before >= 0:
                candidate = entity_counts[idx_before]['entity']
                if candidate not in current_candidates:
                    current_candidates.append(candidate)
                    count += 1
                    if count >= num_samples: break
                idx_before -= 1

        selected_candidates[category] = current_candidates
        logging.info(f"Selected {len(selected_candidates[category])} candidates for category '{category}' (Target index: {base_index}, Approx count: {entity_counts[safe_base_index]['count']})")


    # 2. Ensure uniqueness across categories using priority
    final_selected_entities = defaultdict(list)
    used_entities = set()
    # Define priority: extremes first, then middle, then in-betweens
    priority_order = ['min', 'max', 'mid', 'low_mid', 'high_mid']

    logging.info("Applying uniqueness constraint...")
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
        # Log if fewer than num_samples were assigned due to overlaps
        if added_count < len(candidates) and added_count < num_samples:
             logging.warning(f"  Category '{category}': Fewer than targeted ({num_samples}) unique entities assigned ({added_count}) due to overlaps with higher priority categories.")

    return dict(final_selected_entities)


def get_entities_by_frequent_changes(endpoint, property_uri, query_template=QUERY_PROPERTY_CHANGE_COUNTS, num_samples=5):
    """
    Finds entities where a specific property was frequently changed.

    Args:
        endpoint (str): SPARQL endpoint URL.
        property_uri (str): The full URI of the property to check changes for.
        query_template (str): SPARQL query template (must return ?entity ?changeCount).
        num_samples (int): Number of top entities to retrieve.

    Returns:
        list: A list of OMID strings for entities with the most changes for that property.
              Returns empty list on error or if no entities are found.
    """
    logging.info(f"Finding entities with frequent changes for property: {property_uri}")
    query = query_template.replace("#PROPERTY_URI#", property_uri)
    query += f"\nLIMIT {num_samples}"

    results_data = execute_sparql_query(endpoint, query)
    if not results_data or 'results' not in results_data or 'bindings' not in results_data['results']:
        logging.error(f"Failed to retrieve change counts for {property_uri} or data format is incorrect.")
        return []

    bindings = results_data['results']['bindings']
    selected_entities = []
    for bind in bindings:
         try:
            entity = bind['entity']['value']
            selected_entities.append(entity)
         except (KeyError, ValueError) as e:
            logging.warning(f"Skipping binding due to parsing error ({e}): {bind}")
            continue

    logging.info(f"Selected {len(selected_entities)} entities with most changes for {property_uri}.")
    return selected_entities


if __name__ == "__main__":
    print("-" * 60)
    logging.info(f"Starting entity selection from Provenance Endpoint:")
    logging.info(f"{PROVENANCE_SPARQL_ENDPOINT}")
    print("-" * 60)

    # 1. Find entities based on snapshot count (History Length)
    snapshot_counts_entities = get_entities_by_snapshot_count(PROVENANCE_SPARQL_ENDPOINT, num_samples=5)
    print("\nEntities selected by Snapshot Count:")
    if snapshot_counts_entities:
        for category, omids in snapshot_counts_entities.items():
            print(f"  {category.capitalize()}:")
            for omid in omids:
                print(f"    - {omid}")
    else:
        print("  No entities found or error occurred.")
    print("-" * 60)

    # 2. Find entities based on frequent title changes (Change Frequency/Type)
    title_prop_uri = "http://purl.org/dc/terms/title"
    frequent_title_change_entities = get_entities_by_frequent_changes(PROVENANCE_SPARQL_ENDPOINT, title_prop_uri, num_samples=5)
    print("\nEntities selected by Frequent Title Changes:")
    if frequent_title_change_entities:
        for omid in frequent_title_change_entities:
            print(f"  - {omid}")
    else:
        print("  No entities found or error occurred.")
    print("-" * 60)

    # 3. Find entities based on frequent literal value changes (e.g., for Identifiers)
    literal_prop_uri = "http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue"
    frequent_literal_change_entities = get_entities_by_frequent_changes(PROVENANCE_SPARQL_ENDPOINT, literal_prop_uri, num_samples=5)
    print("\nEntities selected by Frequent Literal Value Changes:")
    if frequent_literal_change_entities:
        for omid in frequent_literal_change_entities:
            print(f"  - {omid}")
    else:
        print("  No entities found or error occurred.")
    print("-" * 60)

    logging.info("Representative entity selection script finished.")