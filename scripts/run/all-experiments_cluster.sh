#!/bin/bash
trap '
  trap - INT # restore default INT handler
  kill -s INT "$$"
' INT

experiments=("noplan-overhead_cluster.sh" "planning-benefit_cluster.sh" "experiment-variable-pipeline_cluster.sh")
iterations=3

for i in `seq 1 $iterations`
do
  for c in "${experiments[@]}"
  do
    /bin/bash $c || true
  done
done
