#!/bin/sh

# input: node-id
# build: container ip
id=$1
let idx=id-1

ip=$(kubectl get pod meta-service-$idx -n databend-system -o json | jq -r .status.podIP)

echo $ip
