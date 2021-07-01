#!/bin/bash
STATSDIR="~/planning-dynamic-spark-supplemental/stats"
[ -d "${STATSDIR}" ] || mkdir "${STATSDIR}"

OUTDIR="${STATSDIR}/$(date '+%d-%m-%YT%Hh%m')"
[ -d "${OUTDIR}" ] || mkdir "${OUTDIR}"

trap '
  trap - INT # restore default INT handler
  kill -s INT "$$"
' INT

configs=("planning-benefit-complex.sh" "planning-benefit-middle.sh" "planning-benefit-simple.sh")

declare -A dbsizes
dbsizes=([1]=80 [2]=40 [4]=20 [5]=16 [8]=10 [10]=8 [16]=5 [20]=4 [40]=2 [80]=1)
indexes=( 1 2 4 5 8 10 16 20 40 80 )
for iterations in "${indexes[@]}"
do
  for conf in "${configs[@]}"
  do
    source $conf

    dbsize=${dbsizes[$iterations]}
    echo "== Benching $conf for $iterations with size times $dbsize =="

    echo "Static iterations for $dbsize * $iterations"
    for itt in `seq 1 $iterations`
    do
      echo "Iteration $itt -> $dbsize"
      /usr/bin/time -f "%e" -ao "${OUTDIR}/${conf}_static_${iterations}.txt" \
        $SPARK_HOME/bin/spark-submit --master $SPARK_MASTER --deploy-mode client \
        --conf "spark.dynamic.experiments.dbsize=${dbsize}" \
        --conf "spark.dynamic.experiments.iterations=1" \
        --class "${MAINCLASS_STATIC}" \
        ~/planning-dynamic-spark-supplemental/jars/$JARNAME.jar "$@"
    done

    echo "Planned iteration for $dbsize * $iterations"
    /usr/bin/time -f "%e" -ao "${OUTDIR}/${conf}_planned_${iterations}.txt" \
        $SPARK_HOME/bin/spark-submit --master $SPARK_MASTER --deploy-mode client \
        --conf "spark.dynamic.experiments.dbsize=${dbsize}" \
        --conf "spark.dynamic.experiments.iterations=${iterations}" \
        --class "${MAINCLASS_PLANNED}" \
        ~/planning-dynamic-spark-supplemental/jars/$JARNAME.jar "$@"

    echo "Dynamic iteration for $dbsize * $iterations"
    /usr/bin/time -f "%e" -ao "${OUTDIR}/${conf}_dynamic_${iterations}.txt" \
        $SPARK_HOME/bin/spark-submit --master $SPARK_MASTER --deploy-mode client \
        --conf "spark.dynamic.experiments.dbsize=${dbsize}" \
        --conf "spark.dynamic.experiments.iterations=${iterations}" \
        --class "${MAINCLASS_DYNAMIC}" \
        ~/planning-dynamic-spark-supplemental/jars/$JARNAME.jar "$@"
  done
done
