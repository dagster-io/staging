#!/bin/bash

# Clean up dagster-test namespaces left on local kind clusters by running integration tests with
# --no-cleanup

kubectl delete `kubectl get ns -o name | grep dagster-test` &
echo "Deleting ns's in the backgound"
