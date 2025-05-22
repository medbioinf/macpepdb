#!/bin/bash
# Checks if the scylla cluster is ready.
# After 5 minutes, the script exits with 1 


timeout=$(date +%s)
timeout=$((timeout + 300)) # 5 minutes timeout
while [[ $(docker compose exec -it scylla-node1 nodetool status | grep -c '^UN ') -ne 2 ]]
do
    echo "Waiting for Scylla nodes to be ready..."
    if [[ $(date +%s) -gt $timeout ]]; then
        echo "Timeout waiting for Scylla nodes to be ready."
        exit 1
    fi
    sleep 5
done