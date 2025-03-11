# PowerShell script to stop test database for time-agnostic-library

# Stop and remove the test database container
if (docker ps -a --format "{{.Names}}" | Select-String -Pattern "^test-virtuoso$") {
    Write-Host "Stopping and removing test-virtuoso container..."
    docker stop test-virtuoso
    docker rm test-virtuoso
}

Write-Host "Test database stopped and removed." 