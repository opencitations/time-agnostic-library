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
CONTAINER_NAME="fuseki-${CORPUS}${SUFFIX}"
DATABASE_DIR="${DATA_DIR}/${CORPUS}/fuseki-data${SUFFIX}"
JENA_VERSION="6.2.0"
JAVA_IMAGE="eclipse-temurin:21.0.8_9-jre-jammy"
TOOLS_DIR="${DATA_DIR}/jena-${JENA_VERSION}"
JENA_HOME="${TOOLS_DIR}/apache-jena-${JENA_VERSION}"
FUSEKI_HOME="${TOOLS_DIR}/apache-jena-fuseki-${JENA_VERSION}"
JENA_HEAP="${JENA_HEAP:-16G}"
FUSEKI_HEAP="${FUSEKI_HEAP:-16G}"
LOAD_THREADS="${LOAD_THREADS:-$(nproc)}"

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

download_distribution() {
    local archive_name="$1"
    local archive_path="${TOOLS_DIR}/${archive_name}"
    local url="https://dlcdn.apache.org/jena/binaries/${archive_name}"

    mkdir -p "${TOOLS_DIR}"
    curl -fsSL "${url}.sha512" -o "${archive_path}.sha512"
    if [ ! -f "${archive_path}" ]; then
        curl -fL "${url}" -o "${archive_path}.part"
        mv "${archive_path}.part" "${archive_path}"
    fi
    (cd "${TOOLS_DIR}" && sha512sum -c "${archive_name}.sha512")
    if [ ! -d "${TOOLS_DIR}/${archive_name%.tar.gz}" ]; then
        tar -xzf "${archive_path}" -C "${TOOLS_DIR}"
    fi
}

echo "=== Fuseki setup (${CORPUS}${SUFFIX}, port ${PORT}) ==="

download_distribution "apache-jena-${JENA_VERSION}.tar.gz"
download_distribution "apache-jena-fuseki-${JENA_VERSION}.tar.gz"
docker pull "${JAVA_IMAGE}" > /dev/null

docker rm -f "${CONTAINER_NAME}" "${CONTAINER_NAME}-indexer" 2>/dev/null || true
rm -rf "${DATABASE_DIR}"
mkdir -p "${DATABASE_DIR}/tmp" "${DATABASE_DIR}/run"

cat > "${DATABASE_DIR}/config.ttl" <<'EOF'
@prefix : <#> .
@prefix fuseki: <http://jena.apache.org/fuseki#> .
@prefix text: <http://jena.apache.org/text#> .
@prefix tdb2: <http://jena.apache.org/2016/tdb#> .

:service a fuseki:Service ;
    fuseki:name "sparql" ;
    fuseki:endpoint [ fuseki:operation fuseki:query ] ;
    fuseki:dataset :text_dataset .

:text_dataset a text:TextDataset ;
    text:dataset :dataset ;
    text:index :index .

:dataset a tdb2:DatasetTDB2 ;
    tdb2:location "/database/TDB2" ;
    tdb2:unionDefaultGraph true .

:index a text:TextIndexLucene ;
    text:directory <file:/database/Lucene> ;
    text:entityMap :entity_map ;
    text:analyzer [
        a text:ConfigurableAnalyzer ;
        text:tokenizer text:WhitespaceTokenizer
    ] .

:entity_map a text:EntityMap ;
    text:entityField "uri" ;
    text:defaultField "updateQuery" ;
    text:map (
        [
            text:field "updateQuery" ;
            text:predicate <https://w3id.org/oc/ontology/hasUpdateQuery>
        ]
    ) .
EOF

echo "Bulk loading N-Quads into TDB2..."
LOAD_START=$(date +%s)
JVM_ARGS="-Xmx${JENA_HEAP}" "${JENA_HOME}/bin/tdb2.xloader" \
    --loc="${DATABASE_DIR}/TDB2" \
    --tmpdir="${DATABASE_DIR}/tmp" \
    --threads="${LOAD_THREADS}" \
    "${DATASET_NQ}" "${PROVENANCE_NQ}" \
    | tee "${DATABASE_DIR}/xloader.log"
LOAD_ELAPSED=$(($(date +%s) - LOAD_START))
QUADS="$(awk '/Quads loaded/ { value = $NF } END { print value }' "${DATABASE_DIR}/xloader.log")"
if [ -z "${QUADS}" ]; then
    echo "Error: tdb2.xloader did not report the number of loaded quads."
    exit 1
fi
rm -rf "${DATABASE_DIR}/tmp"
echo "  Load time: ${LOAD_ELAPSED}s"
echo "  Quads loaded: ${QUADS}"

echo "Building the Lucene index over hasUpdateQuery..."
FT_START=$(date +%s)
docker run --rm --name "${CONTAINER_NAME}-indexer" \
    --user "$(id -u):$(id -g)" \
    --entrypoint java \
    -v "${DATABASE_DIR}:/database" \
    -v "${FUSEKI_HOME}:/fuseki:ro" \
    "${JAVA_IMAGE}" \
    "-Xmx${FUSEKI_HEAP}" \
    -cp /fuseki/fuseki-server.jar \
    jena.textindexer --desc=/database/config.ttl \
    | tee "${DATABASE_DIR}/textindexer.log"
FT_ELAPSED=$(($(date +%s) - FT_START))
echo "  Full-text index time: ${FT_ELAPSED}s"

docker run -d --name "${CONTAINER_NAME}" \
    --user "$(id -u):$(id -g)" \
    --entrypoint java \
    -p "${PORT}:3030" \
    -e FUSEKI_BASE=/database/run \
    -v "${DATABASE_DIR}:/database" \
    -v "${FUSEKI_HOME}:/fuseki:ro" \
    "${JAVA_IMAGE}" \
    "-Xmx${FUSEKI_HEAP}" \
    -jar /fuseki/fuseki-server.jar \
    --config=/database/config.ttl --port=3030 > /dev/null

echo "Waiting for Fuseki to accept connections..."
until curl -fs "http://127.0.0.1:${PORT}/\$/ping" > /dev/null; do
    if [ "$(docker inspect -f '{{.State.Running}}' "${CONTAINER_NAME}")" != "true" ]; then
        docker logs "${CONTAINER_NAME}"
        exit 1
    fi
    sleep 2
done

if ! curl -fsG "http://127.0.0.1:${PORT}/sparql" \
    --data-urlencode 'query=PREFIX text: <http://jena.apache.org/text#> ASK { ?snapshot text:query "DATA" }' \
    -H 'Accept: application/sparql-results+json' \
    | jq -e '.boolean == true' > /dev/null; then
    echo "Error: the full-text index returned no update query."
    exit 1
fi

STORE_BYTES="$(du -sb "${DATABASE_DIR}" | cut -f1)"

cat > "${DATA_DIR}/fuseki_ingestion_time_${CORPUS}${SUFFIX}.json" <<EOF
{
  "fuseki_load_s": ${LOAD_ELAPSED},
  "fuseki_full_text_index_s": ${FT_ELAPSED},
  "quads": ${QUADS},
  "load_threads": ${LOAD_THREADS},
  "jena_version": "${JENA_VERSION}",
  "jena_heap": "${JENA_HEAP}",
  "fuseki_heap": "${FUSEKI_HEAP}",
  "store_bytes": ${STORE_BYTES}
}
EOF

echo ""
echo "Fuseki is running at http://localhost:${PORT}/sparql"
echo "To stop: docker rm -f ${CONTAINER_NAME}"
echo "Next: uv run --group benchmark python benchmark/bear/verify_results.py --corpus ${CORPUS}"
