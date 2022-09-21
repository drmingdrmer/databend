#!/bin/sh

. k8/ubuntu_name.sh

kubectl exec -i -t -n default $ubuntu_name -c ubuntu -- sh -c "$*"
