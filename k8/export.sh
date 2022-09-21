#!/bin/sh


ip=$(k8/container_ip.sh $1)

echo "export $1 from $ip" >&2
mkdir -p k8/exported

cmd='./bin/databend-metactl --export --grpc-api-address '$ip':9191'
./k8/cmd.sh "$cmd"
