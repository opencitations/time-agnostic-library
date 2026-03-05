#!/bin/bash
set -euo pipefail

TRIPLESTORE="${TRIPLESTORE:-virtuoso}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$SCRIPT_DIR"

echo "Stopping $TRIPLESTORE..."
docker compose --profile "$TRIPLESTORE" down -v

echo "Teardown completed for $TRIPLESTORE."
