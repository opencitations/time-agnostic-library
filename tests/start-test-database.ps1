# PowerShell script to start test database for time-agnostic-library

# Get the absolute path of the current directory
$CURRENT_DIR = (Get-Location).Path

# Create test database directory if it doesn't exist
$dbPath = Join-Path -Path $CURRENT_DIR -ChildPath "tests\test_db"

if (-not (Test-Path -Path $dbPath)) {
    Write-Host "Creating database directory..."
    New-Item -Path $dbPath -ItemType Directory -Force | Out-Null
}

# Check if container already exists and remove it if it does
if (docker ps -a --format "{{.Names}}" | Select-String -Pattern "^test-virtuoso$") {
    Write-Host "Removing existing test-virtuoso container..."
    docker rm -f test-virtuoso
}

# Start Virtuoso database
Write-Host "Starting test-virtuoso container..."
docker run -d --name test-virtuoso `
  -p 1109:1111 `
  -p 9999:8890 `
  -e DBA_PASSWORD=dba `
  -e SPARQL_UPDATE=true `
  -v "${dbPath}:/database" `
  openlink/virtuoso-opensource-7:latest

# Wait for database to be ready
Write-Host "Waiting for test database to start..."
Start-Sleep -Seconds 30
Write-Host "Test database should be ready now."

# Set permissions for the 'nobody' user
Write-Host "Setting permissions for the 'nobody' user..."
docker exec test-virtuoso /opt/virtuoso-opensource/bin/isql -U dba -P dba exec="DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('nobody', 7);"

# Grant SPARQL_UPDATE role to SPARQL user
Write-Host "Granting SPARQL_UPDATE role..."
docker exec test-virtuoso /opt/virtuoso-opensource/bin/isql -U dba -P dba exec="DB.DBA.USER_GRANT_ROLE ('SPARQL', 'SPARQL_UPDATE');"

# Load test data
Write-Host "Loading test data..."
uv run python tests/load_test_data.py

Write-Host "Setup completed."
Write-Host "Virtuoso DB: http://localhost:9999/sparql" 