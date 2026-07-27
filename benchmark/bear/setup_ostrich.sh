#!/usr/bin/env bash

# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
CORPUS="${1:-bear-b-daily}"
OSTRICH_DIR="$DATA_DIR/ostrich"
PATCHES_DIR="$OSTRICH_DIR/patches_${CORPUS}"
EVALRUN_DIR="$OSTRICH_DIR/evalrun_${CORPUS}"
QUERIES_DIR="$OSTRICH_DIR/queries"
OSTRICH_REPO="$OSTRICH_DIR/ostrich-repo"
IMAGE_NAME="ostrich-bear"

NUM_VERSIONS="$(cd "${SCRIPT_DIR}" && uv run python -c "import corpora; print(corpora.get('${CORPUS}').num_versions)")" || {
    echo "Error: unknown corpus '${CORPUS}'"
    exit 1
}

# Per corpus, the strategy Pelgrin et al. (2025) measure as the fastest to
# ingest in their Table 6a.
case "${CORPUS}" in
    bear-a)         STRATEGY="interval"; STRATEGY_PARAM=2 ;;
    bear-b-daily)   STRATEGY="interval"; STRATEGY_PARAM=5 ;;
    bear-b-hourly)  STRATEGY="interval"; STRATEGY_PARAM=50 ;;
    bear-b-instant) STRATEGY="time";     STRATEGY_PARAM=20 ;;
    *)
        echo "Error: no snapshot strategy chosen for '${CORPUS}'"
        exit 1
        ;;
esac

echo "=== OSTRICH benchmark setup (${CORPUS}, ${NUM_VERSIONS} versions) ==="

# Check Docker
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running"
    exit 1
fi

# Build OSTRICH Docker image from source
if docker image inspect "$IMAGE_NAME" > /dev/null 2>&1; then
    echo "Docker image '$IMAGE_NAME' already exists, skipping build"
else
    echo "Building OSTRICH Docker image from source..."
    if [ ! -d "$OSTRICH_REPO" ]; then
        git clone --recurse-submodules -b feat/olivier https://github.com/rdfostrich/ostrich.git "$OSTRICH_REPO"
    fi
    docker build -t "$IMAGE_NAME" "$OSTRICH_REPO"
    echo "Docker image '$IMAGE_NAME' built"
fi

# Prepare data in OSTRICH's expected directory structure
IC_SRC="$DATA_DIR/${CORPUS}/IC"
CB_SRC="$DATA_DIR/${CORPUS}/CB"

if [ ! -d "$IC_SRC" ] || [ ! -d "$CB_SRC" ]; then
    echo "Error: ${CORPUS} data not found. Run 'python benchmark/bear/download.py --corpus ${CORPUS}' first."
    exit 1
fi

IC_DST="$PATCHES_DIR/alldata.IC.nt"
CB_DST="$PATCHES_DIR/alldata.CB.nt"

if [ -d "$IC_DST" ] && [ -d "$CB_DST" ]; then
    echo "Data already prepared in $PATCHES_DIR"
else
    echo "Preparing data..."
    mkdir -p "$IC_DST" "$CB_DST"

    # Hard linked, not copied: on BEAR-A that is 332 GB not duplicated.
    prepare() {
        local src="$1" dst="$2" name="$3"
        for f in "$src"/*.nt; do
            [ -e "$f" ] || continue
            local base
            base="$(basename "$f")"
            [ -f "$dst/$base" ] || ln "$f" "$dst/$base" || cp "$f" "$dst/$base"
        done
        echo "  ${name}: $(ls "$dst"/*.nt 2>/dev/null | wc -l) files"
    }

    prepare "$IC_SRC" "$IC_DST" "IC"
    prepare "$CB_SRC" "$CB_DST" "CB"
fi

# Copy query files
mkdir -p "$QUERIES_DIR"
cp -n "$DATA_DIR/queries/p.txt" "$QUERIES_DIR/" 2>/dev/null || true
cp -n "$DATA_DIR/queries/po.txt" "$QUERIES_DIR/" 2>/dev/null || true
echo "Query files ready in $QUERIES_DIR"

# Run OSTRICH ingestion
INGESTION_LOG="$OSTRICH_DIR/ingestion_output_${CORPUS}.txt"
if [ -d "$EVALRUN_DIR" ] && [ "$(ls -A "$EVALRUN_DIR" 2>/dev/null)" ]; then
    echo "OSTRICH store already exists in $EVALRUN_DIR, skipping ingestion"
else
    mkdir -p "$EVALRUN_DIR"
    echo "Running OSTRICH ingestion (${NUM_VERSIONS} versions, strategy=${STRATEGY} ${STRATEGY_PARAM})..."
    docker run --rm \
        --ulimit nofile=65536:65536 \
        -v "$EVALRUN_DIR":/var/evalrun \
        -v "$PATCHES_DIR":/var/patches \
        "$IMAGE_NAME" ingest "$STRATEGY" "$STRATEGY_PARAM" /var/patches 1 "${NUM_VERSIONS}" 2>&1 | tee "$INGESTION_LOG"
    echo "Ingestion complete (log saved to $INGESTION_LOG)"
fi

# Measure OSTRICH store size
STORE_BYTES=$(du -sb "$EVALRUN_DIR" | cut -f1)
STORE_MB=$(awk "BEGIN {printf \"%.1f\", ${STORE_BYTES}/1048576}")
echo "  Store size: ${STORE_BYTES} bytes (${STORE_MB} MB)"
echo "{\"store_bytes\": ${STORE_BYTES}}" > "$DATA_DIR/ostrich_store_size_${CORPUS}.json"

echo ""
echo "=== Setup complete ==="
echo "  OSTRICH store: $EVALRUN_DIR"
echo "  Patches: $PATCHES_DIR"
echo "  Queries: $QUERIES_DIR"
echo ""
echo "Next: python benchmark/bear/run_ostrich_benchmark.py --corpus ${CORPUS}"
