#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
GRANULARITY="${1:-daily}"
IMAGE_NAME="plttud/r43ples:latest"
CONTAINER_NAME="r43ples-bear-${GRANULARITY}"
VOLUME_NAME="r43ples-data-${GRANULARITY}"
R43PLES_PORT=9998

case "$GRANULARITY" in
    daily)   NUM_VERSIONS=89 ;;
    hourly)  NUM_VERSIONS=1299 ;;
    instant) NUM_VERSIONS=21046 ;;
    *) echo "Error: unknown granularity '$GRANULARITY'. Use 'daily', 'hourly', or 'instant'."; exit 1 ;;
esac

wait_for_r43ples() {
    echo "Waiting for R43ples to be ready..."
    for i in $(seq 1 30); do
        if curl -s -o /dev/null -w "" http://localhost:${R43PLES_PORT}/r43ples/sparql 2>/dev/null; then
            echo "  R43ples is ready (took ${i}s)"
            return 0
        fi
        if [ "$i" -eq 30 ]; then
            echo "Error: R43ples failed to start within 30s"
            docker logs "$CONTAINER_NAME" 2>&1 | tail -20
            exit 1
        fi
        sleep 1
    done
}

start_container() {
    docker run -d \
        --name "$CONTAINER_NAME" \
        -p "${R43PLES_PORT}:9998" \
        -v "${VOLUME_NAME}:/r43ples/database" \
        "$IMAGE_NAME" \
        java -Xmx8g -jar r43ples.jar
}

ensure_container_running() {
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "Container ${CONTAINER_NAME} is already running."
    elif docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "Container ${CONTAINER_NAME} exists but is stopped, restarting..."
        docker start "$CONTAINER_NAME"
    else
        echo "Container ${CONTAINER_NAME} not found, recreating..."
        start_container
    fi
    wait_for_r43ples
}

echo "=== R43ples benchmark setup (${GRANULARITY}, ${NUM_VERSIONS} versions) ==="

if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running"
    exit 1
fi

IC_SRC="$DATA_DIR/${GRANULARITY}/IC"
CB_SRC="$DATA_DIR/${GRANULARITY}/CB"

if [ ! -d "$IC_SRC" ] || [ ! -d "$CB_SRC" ]; then
    echo "Error: BEAR-B-${GRANULARITY} data not found. Run 'python benchmark/bear/download.py --granularity ${GRANULARITY}' first."
    exit 1
fi

INGESTION_TIME_FILE="$DATA_DIR/r43ples_ingestion_time_${GRANULARITY}.json"

if [ -f "$INGESTION_TIME_FILE" ]; then
    echo "Ingestion already completed (found $INGESTION_TIME_FILE), skipping."
    echo "Delete the file and the container to re-run."
    ensure_container_running
    echo ""
    echo "Next: python benchmark/bear/run_r43ples_benchmark.py --granularity ${GRANULARITY}"
    exit 0
fi

# Stop and remove any existing container
docker stop "$CONTAINER_NAME" 2>/dev/null || true
docker rm "$CONTAINER_NAME" 2>/dev/null || true

# Pull image if needed
docker pull "$IMAGE_NAME" 2>/dev/null || true

# Start R43ples container with a named volume for persistence
docker volume rm "$VOLUME_NAME" 2>/dev/null || true
docker volume create "$VOLUME_NAME" > /dev/null

echo "Starting R43ples container..."
start_container
wait_for_r43ples

# Run Python ingestion script
INGESTION_START=$(date +%s%N)

python "$SCRIPT_DIR/ingest_r43ples.py" --granularity "$GRANULARITY" --port "$R43PLES_PORT"

INGESTION_END=$(date +%s%N)
INGESTION_NS=$((INGESTION_END - INGESTION_START))
INGESTION_S=$(awk "BEGIN {printf \"%.2f\", ${INGESTION_NS}/1000000000}")

# Measure store size
STORE_BYTES=$(docker exec "$CONTAINER_NAME" du -sb /r43ples/database 2>/dev/null | cut -f1 || echo "0")
STORE_MB=$(awk "BEGIN {printf \"%.1f\", ${STORE_BYTES}/1048576}")
echo "Store size: ${STORE_BYTES} bytes (${STORE_MB} MB)"

# Save timing and size data
cat > "$INGESTION_TIME_FILE" << EOF
{
    "ingestion_s": ${INGESTION_S},
    "store_bytes": ${STORE_BYTES},
    "num_versions": ${NUM_VERSIONS},
    "granularity": "${GRANULARITY}"
}
EOF
echo "Saved: ${INGESTION_TIME_FILE}"

echo ""
echo "=== Setup complete ==="
echo "  R43ples container: ${CONTAINER_NAME} (port ${R43PLES_PORT})"
echo "  Versions: ${NUM_VERSIONS}"
echo ""
echo "Next: python benchmark/bear/run_r43ples_benchmark.py --granularity ${GRANULARITY}"
