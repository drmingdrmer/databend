#!/bin/sh

{
k8/status.sh 1
k8/status.sh 2
k8/status.sh 3
k8/status.sh 4
k8/status.sh 5
} | jq .last_seq
