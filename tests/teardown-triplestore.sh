#!/bin/bash
set -euo pipefail

TRIPLESTORE="${TRIPLESTORE:-virtuoso}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$SCRIPT_DIR"

echo "Stopping $TRIPLESTORE..."

if [ "$TRIPLESTORE" = "qlever" ]; then
    QLEVER_DIR="$SCRIPT_DIR/qlever_data"
    if [ -d "$QLEVER_DIR" ]; then
        cd "$QLEVER_DIR"
        uv run qlever stop --name tal 2>/dev/null || true
        cd "$SCRIPT_DIR"
        rm -rf "$QLEVER_DIR"
    fi
    docker ps -a --filter "publish=41760" --format "{{.ID}}" | xargs -r docker rm -f 2>/dev/null || true
else
    docker compose --profile "$TRIPLESTORE" down -v
fi

echo "Teardown completed for $TRIPLESTORE."
