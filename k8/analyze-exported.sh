#!/bin/sh

fn=$1

echo ""

purged=$(cat $fn | grep '"raft_log"' | grep '"LogMeta"'                                       | jq '.[1].LogMeta.value.LogId.index           ')
last_index=$(cat $fn | grep '"raft_log"' | grep '"Logs"' | tail -n1                           | jq '.[1].Logs.key                            ')
applied=$(cat $fn | grep '"state_machine/' | grep '"StateMachineMeta"' | grep '"LastApplied"' | jq '.[1].StateMachineMeta.value.LogId.index  ')
seq=$(cat $fn | grep '"state_machine/' | grep '"Sequences"'                                   | jq '.[1].Sequences.value                     ')
sm_cnt=$(cat $fn | grep '"state_machine/' | wc -l)

let a_s=applied-seq

echo "$fn:  purged: $purged" "last_index: $last_index" "applied: $applied" "seq: $seq" "apply_seq: $a_s" "sm_count: $sm_cnt"


# critical state:
# ["raft_log",{"LogMeta":{"key":"LastPurged","value":{"LogId":{"term":3054,"index":1500399}}}}]
# ["raft_log",{"Logs":{"key":1502661,"value":{"log_id":{"term":3066,"index":1502661},"payload":{"Normal":{"txid":null,"cmd":{"UpsertKV":{"key":"__fd_clusters/default/default/databend_query/BWGQnHpjducatduNsiRH23","seq":{"GE":1},"value":"AsIs","value_meta":{"expire_at":1661349881}}}}}}}}]
# ["state_machine/1",{"StateMachineMeta":{"key":"LastApplied","value":{"LogId":{"term":3066,"index":1502661}}}}]
# ["state_machine/1",{"Sequences":{"key":"generic-kv","value":863454}}]
