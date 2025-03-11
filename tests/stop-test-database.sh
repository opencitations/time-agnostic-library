#!/bin/bash
# Script to stop test database for time-agnostic-library

# Stop and remove the test database container
if [ "$(docker ps -a -q -f name=test-virtuoso)" ]; then
    echo "Stopping and removing test-virtuoso container..."
    docker stop test-virtuoso
    docker rm test-virtuoso
fi

echo "Test database stopped and removed." 