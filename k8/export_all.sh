#!/bin/sh

mkdir -p k8/exported

./k8/export.sh 1 > k8/exported/n1.txt
./k8/export.sh 2 > k8/exported/n2.txt
./k8/export.sh 3 > k8/exported/n3.txt
./k8/export.sh 4 > k8/exported/n4.txt
./k8/export.sh 5 > k8/exported/n5.txt

./k8/fix_state_machine_id.sh

(
cd k8/exported
grep Sequence fixed* > seq
)

k8/status_all.sh

>k8/exported/analysis
./k8/analyze-exported.sh k8/exported/fixed1 >> k8/exported/analysis
./k8/analyze-exported.sh k8/exported/fixed2 >> k8/exported/analysis
./k8/analyze-exported.sh k8/exported/fixed3 >> k8/exported/analysis
./k8/analyze-exported.sh k8/exported/fixed4 >> k8/exported/analysis
./k8/analyze-exported.sh k8/exported/fixed5 >> k8/exported/analysis
