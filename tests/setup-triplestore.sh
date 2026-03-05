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

if [ "$TRIPLESTORE" = "fuseki" ]; then
    echo "Creating Fuseki TDB2 dataset with union default graph..."
    TMPFILE=$(mktemp /tmp/fuseki-config-XXXXXX.ttl)
    cat > "$TMPFILE" << 'TURTLE'
@prefix :      <#> .
@prefix fuseki: <http://jena.apache.org/fuseki#> .
@prefix tdb2:  <http://jena.apache.org/2016/tdb#> .

:service a fuseki:Service ;
    fuseki:name "tal" ;
    fuseki:endpoint [ fuseki:operation fuseki:query ] ;
    fuseki:endpoint [ fuseki:operation fuseki:update ] ;
    fuseki:endpoint [ fuseki:operation fuseki:gsp-rw ] ;
    fuseki:dataset :dataset .

:dataset a tdb2:DatasetTDB2 ;
    tdb2:location "/fuseki/databases/tal" ;
    tdb2:unionDefaultGraph true .
TURTLE
    curl -sf -X POST "http://127.0.0.1:41740/\$/datasets" \
        -u admin:admin \
        -H 'Content-Type: text/turtle' \
        --data-binary "@$TMPFILE"
    rm -f "$TMPFILE"
    echo ""
fi

if [ "$TRIPLESTORE" = "graphdb" ]; then
    echo "Creating GraphDB repository..."
    TMPFILE=$(mktemp /tmp/graphdb-repo-XXXXXX.ttl)
    cat > "$TMPFILE" << 'TURTLE'
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.
@prefix rep: <http://www.openrdf.org/config/repository#>.
@prefix sr: <http://www.openrdf.org/config/repository/sail#>.
@prefix sail: <http://www.openrdf.org/config/sail#>.
@prefix graphdb: <http://www.ontotext.com/config/graphdb#>.

[] a rep:Repository ;
   rep:repositoryID "tal" ;
   rdfs:label "TAL test repository" ;
   rep:repositoryImpl [
      rep:repositoryType "graphdb:SailRepository" ;
      sr:sailImpl [
         sail:sailType "graphdb:Sail" ;
         graphdb:repository-type "file-repository" ;
      ]
   ].
TURTLE
    curl -sf -X POST "http://127.0.0.1:41750/rest/repositories" \
        -H 'Content-Type: multipart/form-data' \
        -F "config=@$TMPFILE"
    rm -f "$TMPFILE"
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
