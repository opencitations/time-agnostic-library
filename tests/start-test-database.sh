#!/bin/bash
# Script to start test database for time-agnostic-library

# Get the absolute path of the current directory
CURRENT_DIR="$(pwd)"

# Create test database directory if it doesn't exist
mkdir -p "${CURRENT_DIR}/tests/test_db"

# Check if container already exists and remove it if it does
if [ "$(docker ps -a -q -f name=test-virtuoso)" ]; then
    echo "Removing existing test-virtuoso container..."
    docker rm -f test-virtuoso
fi

# Start Virtuoso database
echo "Starting test-virtuoso container..."
docker run -d --name test-virtuoso \
  -p 1109:1111 \
  -p 9999:8890 \
  -e DBA_PASSWORD=dba \
  -e SPARQL_UPDATE=true \
  -v "${CURRENT_DIR}/tests/test_db:/database" \
  openlink/virtuoso-opensource-7:latest

# Wait for database to be ready
echo "Waiting for test database to start..."
sleep 30
echo "Test database should be ready now."

# Set permissions for the 'nobody' user
echo "Setting permissions for the 'nobody' user..."
docker exec test-virtuoso /opt/virtuoso-opensource/bin/isql -U dba -P dba exec="DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('nobody', 7);"

# Grant SPARQL_UPDATE role to SPARQL user
echo "Granting SPARQL_UPDATE role..."
docker exec test-virtuoso /opt/virtuoso-opensource/bin/isql -U dba -P dba exec="DB.DBA.USER_GRANT_ROLE ('SPARQL', 'SPARQL_UPDATE');"

# Load test data using Poetry
echo "Loading test data..."
poetry run python tests/load_test_data.py

echo "Setup completed."
echo "Virtuoso DB: http://localhost:9999/sparql" 