#!/bin/sh


# build data from backup

killall databend-meta
sleep 1

rm -rf ./loss/raft-dir-*
rm -rf ./loss/log-*
./target/debug/databend-metactl --import --raft-dir "loss/raft-dir-1" --db loss/backup-1.txt
./target/debug/databend-metactl --import --raft-dir "loss/raft-dir-2" --db loss/backup-2.txt
./target/debug/databend-metactl --import --raft-dir "loss/raft-dir-3" --db loss/backup-3.txt

# run metasrv.

./target/debug/databend-meta --raft-dir "loss/raft-dir-1" --single --admin-api-address "127.0.0.1:8801" --raft-api-port 28001 --grpc-api-address "127.0.0.1:9901" --log-file-dir "./loss/log-1/" --log-file-level DEBUG &
sleep 1
./target/debug/databend-meta --raft-dir "loss/raft-dir-2" --single --admin-api-address "127.0.0.1:8802" --raft-api-port 28002 --grpc-api-address "127.0.0.1:9902" --log-file-dir "./loss/log-2/" --log-file-level DEBUG &
sleep 1
./target/debug/databend-meta --raft-dir "loss/raft-dir-3" --single --admin-api-address "127.0.0.1:8803" --raft-api-port 28003 --grpc-api-address "127.0.0.1:9903" --log-file-dir "./loss/log-3/" --log-file-level DEBUG &
sleep 1

exit

# push some work load, create/drop table.


./target/debug/databend-metabench --number 5 --client 5 --grpc-api-address 127.0.0.1:9901 --rpc table


# export from online and offline

./target/debug/databend-metactl --export --grpc-api-address "127.0.0.1:9901" --db loss/export-1-online.txt
./target/debug/databend-metactl --export --grpc-api-address "127.0.0.1:9902" --db loss/export-2-online.txt
./target/debug/databend-metactl --export --grpc-api-address "127.0.0.1:9903" --db loss/export-3-online.txt

# killall -9 databend-meta
killall -9 databend-meta
# exit
sleep 1



ps -ef| grep databend-meta


./target/debug/databend-metactl --export --raft-dir "loss/raft-dir-1" --db loss/export-1-offline.txt
./target/debug/databend-metactl --export --raft-dir "loss/raft-dir-2" --db loss/export-2-offline.txt
./target/debug/databend-metactl --export --raft-dir "loss/raft-dir-3" --db loss/export-3-offline.txt

# restart to see if data changes

# ./target/debug/databend-meta --raft-dir "loss/raft-dir" --single &
# sleep 1


# ./target/debug/databend-metactl --export --grpc-api-address "127.0.0.1:9191" --db loss/export-2-offline.json

