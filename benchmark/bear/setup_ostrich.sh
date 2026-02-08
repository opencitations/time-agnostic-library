#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
OSTRICH_DIR="$DATA_DIR/ostrich"
PATCHES_DIR="$OSTRICH_DIR/patches"
EVALRUN_DIR="$OSTRICH_DIR/evalrun"
QUERIES_DIR="$OSTRICH_DIR/queries"
OSTRICH_REPO="$OSTRICH_DIR/ostrich-repo"
IMAGE_NAME="ostrich-bear"

echo "=== OSTRICH benchmark setup ==="

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
        git clone --recurse-submodules https://github.com/rdfostrich/ostrich.git "$OSTRICH_REPO"
    fi
    docker build -t "$IMAGE_NAME" "$OSTRICH_REPO"
    echo "Docker image '$IMAGE_NAME' built"
fi

# Prepare data in OSTRICH's expected directory structure
IC_SRC="$DATA_DIR/daily/IC"
CB_SRC="$DATA_DIR/daily/CB"

if [ ! -d "$IC_SRC" ] || [ ! -d "$CB_SRC" ]; then
    echo "Error: BEAR-B-daily data not found. Run 'python benchmark/bear/download.py' first."
    exit 1
fi

IC_DST="$PATCHES_DIR/alldata.IC.nt"
CB_DST="$PATCHES_DIR/alldata.CB.nt"

if [ -d "$IC_DST" ] && [ -d "$CB_DST" ]; then
    echo "Data already prepared in $PATCHES_DIR"
else
    echo "Preparing data (gunzipping IC and CB files)..."
    mkdir -p "$IC_DST" "$CB_DST"

    for gz in "$IC_SRC"/*.nt.gz; do
        base="$(basename "$gz" .gz)"
        if [ ! -f "$IC_DST/$base" ]; then
            gunzip -c "$gz" > "$IC_DST/$base"
        fi
    done
    echo "  IC: $(ls "$IC_DST"/*.nt | wc -l) files"

    for gz in "$CB_SRC"/*.nt.gz; do
        base="$(basename "$gz" .gz)"
        if [ ! -f "$CB_DST/$base" ]; then
            gunzip -c "$gz" > "$CB_DST/$base"
        fi
    done
    echo "  CB: $(ls "$CB_DST"/*.nt | wc -l) files"
fi

# Copy query files
mkdir -p "$QUERIES_DIR"
cp -n "$DATA_DIR/queries/p.txt" "$QUERIES_DIR/" 2>/dev/null || true
cp -n "$DATA_DIR/queries/po.txt" "$QUERIES_DIR/" 2>/dev/null || true
echo "Query files ready in $QUERIES_DIR"

# Run OSTRICH ingestion
INGESTION_LOG="$OSTRICH_DIR/ingestion_output.txt"
if [ -d "$EVALRUN_DIR" ] && [ "$(ls -A "$EVALRUN_DIR" 2>/dev/null)" ]; then
    echo "OSTRICH store already exists in $EVALRUN_DIR, skipping ingestion"
else
    mkdir -p "$EVALRUN_DIR"
    echo "Running OSTRICH ingestion (89 versions, strategy=never)..."
    docker run --rm \
        -v "$EVALRUN_DIR":/var/evalrun \
        -v "$PATCHES_DIR":/var/patches \
        "$IMAGE_NAME" ingest never 0 /var/patches 1 89 2>&1 | tee "$INGESTION_LOG"
    echo "Ingestion complete (log saved to $INGESTION_LOG)"
fi

echo ""
echo "=== Setup complete ==="
echo "  OSTRICH store: $EVALRUN_DIR"
echo "  Patches: $PATCHES_DIR"
echo "  Queries: $QUERIES_DIR"
echo ""
echo "Next: python benchmark/bear/run_ostrich_benchmark.py"
