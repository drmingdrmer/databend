#!/bin/sh

cat seq | awk '{print substr($0, 8)}' | jq .[1].Sequences.value | awk '{print $0-prev; prev=$0}'
