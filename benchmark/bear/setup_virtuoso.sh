#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
CORPUS="${1:-bear-b-daily}"
# Optional infix of a partial conversion, as produced by convert_to_ocdm.py
# --versions: "4v" loads dataset.4v.nq.gz instead of dataset.nq.gz.
INFIX="${2:-}"
SUFFIX="${INFIX:+.${INFIX}}"
CONTAINER_NAME="virtuoso-${CORPUS}${SUFFIX}"
VOLUME_NAME="virtuoso-data-${CORPUS}${SUFFIX}"

PORT="$(cd "${SCRIPT_DIR}" && uv run python -c "import corpora; print(corpora.get('${CORPUS}').port)")" || {
    echo "Error: unknown corpus '${CORPUS}'"
    exit 1
}

DATASET_NQ="${DATA_DIR}/${CORPUS}/dataset${SUFFIX}.nq.gz"
PROVENANCE_NQ="${DATA_DIR}/${CORPUS}/provenance${SUFFIX}.nq.gz"

for required in "${DATASET_NQ}" "${PROVENANCE_NQ}"; do
    if [ ! -f "${required}" ]; then
        echo "Error: ${required} not found. Run convert_to_ocdm.py --corpus ${CORPUS} first."
        exit 1
    fi
done

# Virtuoso keeps the working set in its buffer pool: 10k buffers per GB given.
BUFFERS="${VIRTUOSO_BUFFERS:-4000000}"
DIRTY="${VIRTUOSO_DIRTY_BUFFERS:-3000000}"

isql() {
    docker exec "${CONTAINER_NAME}" /opt/virtuoso-opensource/bin/isql -U dba -P dba \
        exec="$1"
}

echo "=== Virtuoso setup (${CORPUS}${SUFFIX}, port ${PORT}) ==="

docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true
docker volume rm "${VOLUME_NAME}" 2>/dev/null || true
docker volume create "${VOLUME_NAME}" > /dev/null

docker run -d --name "${CONTAINER_NAME}" \
    -p "${PORT}:8890" \
    -v "${VOLUME_NAME}:/database" \
    -v "${DATA_DIR}/${CORPUS}:/staging:ro" \
    -e DBA_PASSWORD=dba \
    -e VIRT_Parameters_NumberOfBuffers="${BUFFERS}" \
    -e VIRT_Parameters_MaxDirtyBuffers="${DIRTY}" \
    -e VIRT_Parameters_MaxCheckpointRemap=$((BUFFERS / 4)) \
    -e VIRT_Parameters_DirsAllowed="., /database, /staging" \
    -e VIRT_SPARQL_ResultSetMaxRows=10000000 \
    -e VIRT_SPARQL_MaxQueryExecutionTime=3600 \
    openlink/virtuoso-opensource-7:7.2.16 > /dev/null

echo "Waiting for Virtuoso to accept connections..."
until isql "status();" > /dev/null 2>&1; do sleep 2; done

# The anonymous SPARQL endpoint needs read access to the loaded graphs. The
# grant is already in place in recent images, and isql reports a failed
# statement without failing, so its outcome is ignored here on purpose.
isql "GRANT SPARQL_SELECT TO \"SPARQL\";" > /dev/null 2>&1 || true

echo "Bulk loading N-Quads..."
LOAD_START=$(date +%s)
isql "ld_dir('/staging', 'dataset${SUFFIX}.nq.gz', 'http://bear-benchmark.org/data/');" > /dev/null
isql "ld_dir('/staging', 'provenance${SUFFIX}.nq.gz', 'http://bear-benchmark.org/prov/');" > /dev/null
# One loader per core: rdf_loader_run takes one file at a time from the queue.
LOADERS="$(nproc)"
for _ in $(seq "${LOADERS}"); do
    docker exec -d "${CONTAINER_NAME}" /opt/virtuoso-opensource/bin/isql -U dba -P dba \
        exec="rdf_loader_run();"
done
until [ "$(isql "SELECT COUNT(*) FROM DB.DBA.LOAD_LIST WHERE ll_state <> 2;" \
    | grep -Eo '^[0-9]+' | head -1)" = "0" ]; do
    sleep 10
done
isql "checkpoint;" > /dev/null
LOAD_ELAPSED=$(($(date +%s) - LOAD_START))
echo "  Load time: ${LOAD_ELAPSED}s"

# isql returns success even when a statement fails, so a load that never
# happened has to be caught by looking at what actually landed in the store.
QUEUED="$(isql "SELECT COUNT(*) FROM DB.DBA.LOAD_LIST;" | grep -Eo '^[0-9]+' | head -1)"
if [ "${QUEUED:-0}" -lt 2 ]; then
    echo "Error: the loader queue holds ${QUEUED:-0} files instead of 2."
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

EXPECTED="$(zcat "${DATASET_NQ}" | wc -l)"
TRIPLES="$(isql "SPARQL SELECT COUNT(*) WHERE { GRAPH ?g { ?s ?p ?o } };" \
    | grep -Eo '^[0-9]+' | head -1)"
echo "  Triples: ${TRIPLES} (dataset alone has ${EXPECTED} lines)"
if [ "${TRIPLES:-0}" -lt "${EXPECTED}" ]; then
    echo "Error: fewer triples than the dataset file holds, the load is incomplete."
    exit 1
fi

# The free-text index over object literals is what turns the search through the
# stored update queries from a scan into a lookup. It is not built by default,
# and after a bulk load it has to be forced: the incremental updater only runs
# on a timer.
echo "Building the free-text index over literals..."
FT_START=$(date +%s)
isql "DB.DBA.RDF_OBJ_FT_RULE_ADD(null, null, 'All');" > /dev/null
isql "DB.DBA.VT_BATCH_UPDATE('DB.DBA.RDF_OBJ', 'OFF', null);" > /dev/null
isql "DB.DBA.VT_INC_INDEX_DB_DBA_RDF_OBJ();" > /dev/null
isql "checkpoint;" > /dev/null
FT_ELAPSED=$(($(date +%s) - FT_START))
echo "  Free-text index time: ${FT_ELAPSED}s"

# -L because /database is a symlink into the image, and without it du would
# measure the link itself.
STORE_BYTES="$(docker exec "${CONTAINER_NAME}" du -sbL /database | cut -f1)"

cat > "${DATA_DIR}/virtuoso_ingestion_time_${CORPUS}${SUFFIX}.json" <<EOF
{
  "virtuoso_load_s": ${LOAD_ELAPSED},
  "virtuoso_full_text_index_s": ${FT_ELAPSED},
  "triples": ${TRIPLES},
  "loaders": ${LOADERS},
  "store_bytes": ${STORE_BYTES}
}
EOF

echo ""
echo "Virtuoso is running at http://localhost:${PORT}/sparql"
echo "To stop: docker rm -f ${CONTAINER_NAME}"
echo "Next: python benchmark/bear/verify_results.py --corpus ${CORPUS}"
