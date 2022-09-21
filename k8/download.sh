#!/bin/sh


id=$1
let idx=id-1

fn=$2
local_fn=$3

kubectl exec -n databend-system meta-service-$idx -ti -- cat "$fn" > "$local_fn"
