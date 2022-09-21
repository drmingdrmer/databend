#!/bin/sh

# input: node-id
# build: container ip
id=$1
let idx=id-1

kubectl -n databend-system exec -ti meta-service-$idx -- bash
