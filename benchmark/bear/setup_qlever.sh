#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
QLEVER_DIR="${DATA_DIR}/qlever"
GRANULARITY="daily"
PORT=7001

DATASET_NQ="${DATA_DIR}/${GRANULARITY}/dataset.nq"
PROVENANCE_NQ="${DATA_DIR}/${GRANULARITY}/provenance.nq"
MERGED_NQ="${QLEVER_DIR}/bear-${GRANULARITY}.nq"

if [ ! -f "${DATASET_NQ}" ]; then
    echo "Error: ${DATASET_NQ} not found. Run convert_to_ocdm.py first."
    exit 1
fi

if [ ! -f "${PROVENANCE_NQ}" ]; then
    echo "Error: ${PROVENANCE_NQ} not found. Run convert_to_ocdm.py first."
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

echo "Building QLever index..."
INDEX_START=$(date +%s)
${QLEVER} index \
    --name bear-benchmark \
    --format nq \
    --input-files "bear-${GRANULARITY}.nq" \
    --cat-input-files "cat bear-${GRANULARITY}.nq" \
    --settings-json '{ "prefixes-external": ["http://dbpedia.org/", "http://bear-benchmark.org/"] }' \
    --system docker \
    --overwrite-existing
INDEX_END=$(date +%s)
INDEX_ELAPSED=$((INDEX_END - INDEX_START))
echo "  Indexing time: ${INDEX_ELAPSED}s"
echo "{\"qlever_indexing_s\": ${INDEX_ELAPSED}}" > "${DATA_DIR}/qlever_indexing_time.json"

echo "Starting QLever server on port ${PORT}..."
${QLEVER} start \
    --name bear-benchmark \
    --description "BEAR-B ${GRANULARITY} benchmark" \
    --port "${PORT}" \
    --access-token "" \
    --memory-for-queries 16G \
    --cache-max-size 8G \
    --timeout 120s \
    --kill-existing-with-same-port \
    --system docker

echo ""
echo "QLever is running at http://localhost:${PORT}"
echo "To stop: cd ${QLEVER_DIR} && ${QLEVER} stop --name bear-benchmark --system docker"
echo "To verify: curl -s 'http://localhost:${PORT}' --data-urlencode 'query=SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }'"
