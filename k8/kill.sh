#!/bin/sh

# input: node-id
# build: container ip
id=$1
let idx=id-1

# kubectl delete pod meta-service-$idx -n databend-system

# exit
kubectl delete pod meta-service-$idx -n databend-system --force
