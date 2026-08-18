#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
CORPUS="${1:-bear-b-daily}"
INFIX="${2:-}"
SUFFIX="${INFIX:+.${INFIX}}"
CONTAINER_NAME="virtuoso-${CORPUS}${SUFFIX}"
DATABASE_DIR="${DATA_DIR}/${CORPUS}/virtuoso-data${SUFFIX}"

PORT="$(cd "${SCRIPT_DIR}" && uv run python -c "import corpora; print(corpora.get('${CORPUS}').port)")" || {
    echo "Error: unknown corpus '${CORPUS}'"
    exit 1
}

DATASET_NQ="${DATA_DIR}/${CORPUS}/dataset${SUFFIX}.nq.gz"
PROVENANCE_NQ="${DATA_DIR}/${CORPUS}/provenance${SUFFIX}.nq.gz"
PARTS_DIR="${DATA_DIR}/${CORPUS}/parts${SUFFIX}"
PART_LINES="${PART_LINES:-20000000}"
UPDATE_QUERY_PREDICATE="https://w3id.org/oc/ontology/hasUpdateQuery"

for required in "${DATASET_NQ}" "${PROVENANCE_NQ}"; do
    if [ ! -f "${required}" ]; then
        echo "Error: ${required} not found. Run convert_to_ocdm.py --corpus ${CORPUS} first."
        exit 1
    fi
done

split_into_parts() {
    local src="$1" dst="$2" name="$3"
    rm -rf "${dst}"
    mkdir -p "${dst}"
    zcat "${src}" | split -l "${PART_LINES}" -d -a 4 \
        --filter='gzip -1 > $FILE.nq.gz' - "${dst}/part-"
    echo "  ${name}: $(ls "${dst}" | wc -l) parts"
}

BUFFER_KB=8
FREE_KB="$(awk '/MemAvailable/ {print $2}' /proc/meminfo)"
BUFFERS=$(( FREE_KB * 2 / 3 / BUFFER_KB ))
DIRTY=$(( BUFFERS * 3 / 4 ))

isql() {
    docker exec "${CONTAINER_NAME}" /opt/virtuoso-opensource/bin/isql -U dba -P dba \
        exec="$1"
}

echo "=== Virtuoso setup (${CORPUS}${SUFFIX}, port ${PORT}) ==="

docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true
rm -rf "${DATABASE_DIR}"
mkdir -p "${DATABASE_DIR}"

docker run -d --name "${CONTAINER_NAME}" \
    -p "${PORT}:8890" \
    -v "${DATABASE_DIR}:/database" \
    -v "${DATA_DIR}/${CORPUS}:/staging:ro" \
    -e DBA_PASSWORD=dba \
    -e VIRT_Parameters_NumberOfBuffers="${BUFFERS}" \
    -e VIRT_Parameters_MaxDirtyBuffers="${DIRTY}" \
    -e VIRT_Parameters_MaxCheckpointRemap=$((BUFFERS / 4)) \
    -e VIRT_Parameters_DirsAllowed="., /database, /staging" \
    -e VIRT_SPARQL_ResultSetMaxRows=10000000 \
    -e VIRT_SPARQL_MaxQueryExecutionTime=3600 \
    openlink/virtuoso-opensource-7:7.2.17 > /dev/null

echo "Waiting for Virtuoso to accept connections..."
until isql "status();" > /dev/null 2>&1; do sleep 2; done

isql "GRANT SPARQL_SELECT TO \"SPARQL\";" > /dev/null 2>&1 || true

echo "Bulk loading N-Quads..."
LOAD_START=$(date +%s)
split_into_parts "${DATASET_NQ}" "${PARTS_DIR}/dataset" "dataset"
split_into_parts "${PROVENANCE_NQ}" "${PARTS_DIR}/provenance" "provenance"
isql "ld_dir('/staging/parts${SUFFIX}/dataset', '*.nq.gz', 'http://bear-benchmark.org/data/');" > /dev/null
isql "ld_dir('/staging/parts${SUFFIX}/provenance', '*.nq.gz', 'http://bear-benchmark.org/prov/');" > /dev/null
# One loader per core: rdf_loader_run takes one file at a time from the queue.
LOADERS="$(nproc)"
LOADER_PIDS=()
for _ in $(seq "${LOADERS}"); do
    docker exec "${CONTAINER_NAME}" /opt/virtuoso-opensource/bin/isql -U dba -P dba \
        exec="rdf_loader_run();" > /dev/null &
    LOADER_PIDS+=("$!")
done
for loader_pid in "${LOADER_PIDS[@]}"; do
    wait "${loader_pid}"
done
DEADLOCKED="$(isql "SELECT COUNT(*) FROM DB.DBA.LOAD_LIST WHERE ll_error LIKE '40001%';" \
    | grep -Eo '^[0-9]+' | head -1)"
if [ "${DEADLOCKED:-0}" != "0" ]; then
    echo "  Retrying ${DEADLOCKED} file(s) interrupted by deadlocks..."
    isql "UPDATE DB.DBA.LOAD_LIST SET ll_state = 0, ll_error = NULL WHERE ll_error LIKE '40001%';" > /dev/null
    isql "rdf_loader_run();" > /dev/null
fi
isql "checkpoint;" > /dev/null
LOAD_ELAPSED=$(($(date +%s) - LOAD_START))
echo "  Load time: ${LOAD_ELAPSED}s"

# isql returns success even when a statement fails, so a load that never
# happened has to be caught by looking at what actually landed in the store.
PARTS_TOTAL="$(find "${PARTS_DIR}" -name '*.nq.gz' | wc -l)"
QUEUED="$(isql "SELECT COUNT(*) FROM DB.DBA.LOAD_LIST;" | grep -Eo '^[0-9]+' | head -1)"
if [ "${QUEUED:-0}" -lt "${PARTS_TOTAL}" ]; then
    echo "Error: the loader queue holds ${QUEUED:-0} files instead of ${PARTS_TOTAL}."
    echo "       Check that /staging is listed in DirsAllowed."
    exit 1
fi

FAILED="$(isql "SELECT COUNT(*) FROM DB.DBA.LOAD_LIST WHERE ll_error IS NOT NULL;" \
    | grep -Eo '^[0-9]+' | head -1)"
if [ "${FAILED:-0}" != "0" ]; then
    echo "Error: ${FAILED} file(s) failed to load:"
    isql "SELECT ll_file, ll_error FROM DB.DBA.LOAD_LIST WHERE ll_error IS NOT NULL;"
    exit 1
fi

SOURCE_LINES="$(zcat "${DATASET_NQ}" | wc -l)"
TRIPLES="$(isql "SPARQL SELECT COUNT(*) WHERE { GRAPH <http://bear-benchmark.org/data/> { ?s ?p ?o } };" \
    | grep -Eo '^[0-9]+' | head -1)"
echo "  Distinct triples in the data graph: ${TRIPLES} (the dataset file holds ${SOURCE_LINES} lines)"
if [ "${TRIPLES:-0}" = "0" ]; then
    echo "Error: the data graph is empty."
    exit 1
fi

rm -rf "${PARTS_DIR}"

echo "Building the free-text index over update queries..."
FT_START=$(date +%s)
isql "DB.DBA.RDF_OBJ_FT_RULE_ADD(null, '${UPDATE_QUERY_PREDICATE}', 'BEAR update queries');" > /dev/null
isql "DB.DBA.VT_BATCH_UPDATE('DB.DBA.RDF_OBJ', 'OFF', null);" > /dev/null
isql "DB.DBA.VT_INC_INDEX_DB_DBA_RDF_OBJ();" > /dev/null
isql "checkpoint;" > /dev/null
FT_ELAPSED=$(($(date +%s) - FT_START))
echo "  Free-text index time: ${FT_ELAPSED}s"

STORE_BYTES="$(docker exec "${CONTAINER_NAME}" du -sbL /database | cut -f1)"

cat > "${DATA_DIR}/virtuoso_ingestion_time_${CORPUS}${SUFFIX}.json" <<EOF
{
  "virtuoso_load_s": ${LOAD_ELAPSED},
  "virtuoso_full_text_index_s": ${FT_ELAPSED},
  "triples": ${TRIPLES},
  "source_lines": ${SOURCE_LINES},
  "loaders": ${LOADERS},
  "deadlock_retries": ${DEADLOCKED},
  "buffers": ${BUFFERS},
  "store_bytes": ${STORE_BYTES}
}
EOF

echo ""
echo "Virtuoso is running at http://localhost:${PORT}/sparql"
echo "To stop: docker rm -f ${CONTAINER_NAME}"
echo "Next: python benchmark/bear/verify_results.py --corpus ${CORPUS}"
