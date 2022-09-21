#!/bin/sh

fn=$1

sm_cnt=$(cat $fn | grep '"state_machine/' | wc -l)

echo $sm_cnt
