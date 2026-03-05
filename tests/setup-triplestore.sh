#!/bin/bash
set -euo pipefail

TRIPLESTORE="${TRIPLESTORE:-virtuoso}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$SCRIPT_DIR"

echo "Starting $TRIPLESTORE..."
docker compose --profile "$TRIPLESTORE" up -d --wait

if [ "$TRIPLESTORE" = "virtuoso" ]; then
    CONTAINER="$(docker compose ps --format '{{.Name}}' --status running | grep virtuoso)"
    echo "Configuring Virtuoso permissions..."
    docker exec "$CONTAINER" /opt/virtuoso-opensource/bin/isql -U dba -P dba \
        exec="DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('nobody', 7);"
    docker exec "$CONTAINER" /opt/virtuoso-opensource/bin/isql -U dba -P dba \
        exec="DB.DBA.USER_GRANT_ROLE ('SPARQL', 'SPARQL_UPDATE');"
fi

if [ "$TRIPLESTORE" = "blazegraph" ]; then
    ENDPOINT="http://127.0.0.1:41730/bigdata"
    echo "Creating Blazegraph quads namespace with text index..."
    curl -sf -X POST "$ENDPOINT/namespace" \
        -H 'Content-Type: application/xml' \
        --data '<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
<properties>
  <entry key="com.bigdata.rdf.sail.namespace">tal</entry>
  <entry key="com.bigdata.rdf.store.AbstractTripleStore.quads">true</entry>
  <entry key="com.bigdata.rdf.store.AbstractTripleStore.textIndex">true</entry>
  <entry key="com.bigdata.rdf.sail.truthMaintenance">false</entry>
  <entry key="com.bigdata.rdf.store.AbstractTripleStore.axiomsClass">com.bigdata.rdf.axioms.NoAxioms</entry>
</properties>'
    echo ""
fi

echo "Loading test data..."
cd "$PROJECT_DIR"
TRIPLESTORE="$TRIPLESTORE" uv run python tests/load_test_data.py

if [ "$TRIPLESTORE" = "virtuoso" ]; then
    CONTAINER="$(cd "$SCRIPT_DIR" && docker compose ps --format '{{.Name}}' --status running | grep virtuoso)"
    echo "Running Virtuoso checkpoint..."
    docker exec "$CONTAINER" /opt/virtuoso-opensource/bin/isql -U dba -P dba exec="checkpoint;"
fi

echo "Setup completed for $TRIPLESTORE."
