#!/bin/sh

ip=$(k8/container_ip.sh $1)

cmd='curl '$ip':28002/v1/ctrl/trigger_snapshot'
./k8/cmd.sh "$cmd"

