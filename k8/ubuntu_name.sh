#!/bin/sh

if [ ".$ubuntu_name" = "." ]; then
    # find out ubuntu container name
    ubuntu_name=$(kubectl get pod -A | grep ubuntu | awk '{print $2}')
    export ubuntu_name=$ubuntu_name
fi
