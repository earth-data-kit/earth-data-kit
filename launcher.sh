#!/bin/bash

# Usage: ./launcher.sh script.py arg1 arg2 ...

# Get the script and arguments
SCRIPT="$1"
shift
ARGS="$@"

# Get the container name (assume it's the current directory name)
CONTAINER_NAME="edk"

# Find the container ID by name (assume only one container matches)
CONTAINER_ID=$(docker ps --filter "name=${CONTAINER_NAME}" --format "{{.ID}}" | head -n 1)

if [ -z "$CONTAINER_ID" ]; then
    echo "No running container found with name: $CONTAINER_NAME"
    exit 1
fi

# Run the script inside the container in /app/workspace
docker exec -it "$CONTAINER_ID" bash -c "cd /app/workspace && python $SCRIPT $ARGS"
