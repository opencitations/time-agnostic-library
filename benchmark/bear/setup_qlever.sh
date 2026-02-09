#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
QLEVER_DIR="${DATA_DIR}/qlever"
GRANULARITY="${1:-daily}"
PORT=7001
INDEX_NAME="bear-${GRANULARITY}"

DATASET_NQ="${DATA_DIR}/${GRANULARITY}/dataset.nq"
PROVENANCE_NQ="${DATA_DIR}/${GRANULARITY}/provenance.nq"
MERGED_NQ="${QLEVER_DIR}/${INDEX_NAME}.nq"

if [ ! -f "${DATASET_NQ}" ]; then
    echo "Error: ${DATASET_NQ} not found. Run convert_to_ocdm.py --granularity ${GRANULARITY} first."
    exit 1
fi

if [ ! -f "${PROVENANCE_NQ}" ]; then
    echo "Error: ${PROVENANCE_NQ} not found. Run convert_to_ocdm.py --granularity ${GRANULARITY} first."
    exit 1
fi

QLEVER="qlever"
if command -v uv &> /dev/null && uv run qlever --help &> /dev/null; then
    QLEVER="uv run qlever"
elif ! command -v qlever &> /dev/null; then
    echo "Error: qlever CLI not found. Install with: uv add --dev qlever"
    exit 1
fi

if ! command -v docker &> /dev/null; then
    echo "Error: docker not found. QLever requires Docker for indexing and serving."
    exit 1
fi

echo "Pulling QLever Docker image..."
docker pull adfreiburg/qlever

mkdir -p "${QLEVER_DIR}"

echo "Merging dataset and provenance into single N-Quads file..."
cat "${DATASET_NQ}" "${PROVENANCE_NQ}" > "${MERGED_NQ}"
echo "  Merged file: ${MERGED_NQ} ($(wc -l < "${MERGED_NQ}") lines)"

cd "${QLEVER_DIR}"

# Stop any existing QLever instance on the same port
${QLEVER} stop --name "${INDEX_NAME}" --system docker 2>/dev/null || true

echo "Building QLever index (${GRANULARITY})..."
INDEX_START=$(date +%s)
${QLEVER} index \
    --name "${INDEX_NAME}" \
    --format nq \
    --input-files "${INDEX_NAME}.nq" \
    --cat-input-files "cat ${INDEX_NAME}.nq" \
    --settings-json '{ "prefixes-external": ["http://dbpedia.org/", "http://bear-benchmark.org/"] }' \
    --system docker \
    --overwrite-existing
INDEX_END=$(date +%s)
INDEX_ELAPSED=$((INDEX_END - INDEX_START))
echo "  Indexing time: ${INDEX_ELAPSED}s"
echo "{\"qlever_indexing_s\": ${INDEX_ELAPSED}}" > "${DATA_DIR}/qlever_indexing_time_${GRANULARITY}.json"

echo "Starting QLever server on port ${PORT}..."
# Remove any Docker container already bound to the target port (e.g. a different granularity)
docker ps -a --filter "publish=${PORT}" --format "{{.ID}}" | xargs -r docker rm -f 2>/dev/null || true
${QLEVER} start \
    --name "${INDEX_NAME}" \
    --description "BEAR-B ${GRANULARITY} benchmark" \
    --port "${PORT}" \
    --access-token "" \
    --memory-for-queries 16G \
    --cache-max-size 8G \
    --timeout 120s \
    --kill-existing-with-same-port \
    --system docker

echo ""
echo "QLever is running at http://localhost:${PORT} (index: ${INDEX_NAME})"
echo "To stop: cd ${QLEVER_DIR} && ${QLEVER} stop --name ${INDEX_NAME} --system docker"
echo "To verify: curl -s 'http://localhost:${PORT}' --data-urlencode 'query=SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }'"
