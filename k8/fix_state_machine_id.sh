#!/bin/sh

script='{gsub("state_machine/[0-9]+", "state_machine/1", $0); print $0}'

cat k8/exported/n1.txt | awk "$script" > k8/exported/fixed1
cat k8/exported/n2.txt | awk "$script" > k8/exported/fixed2
cat k8/exported/n3.txt | awk "$script" > k8/exported/fixed3
cat k8/exported/n3.txt | awk "$script" > k8/exported/fixed4
cat k8/exported/n5.txt | awk "$script" > k8/exported/fixed5
