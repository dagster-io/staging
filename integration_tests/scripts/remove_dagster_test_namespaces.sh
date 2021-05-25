#!/bin/bash

kubectl delete `kubectl get ns -o name | grep dagster-test` &
echo "Deleting ns's in the backgound"
