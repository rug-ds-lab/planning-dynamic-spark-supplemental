#!/bin/bash
STATSDIR="~/planning-dynamic-spark-supplemental/stats"
[ -d "${STATSDIR}" ] || mkdir "${STATSDIR}"

trap '
  trap - INT # restore default INT handler
  kill -s INT "$$"
' INT

configs=("noplan-overhead-complex.sh" "noplan-overhead-middle.sh" "noplan-overhead-simple.sh")
iterations=4
dbsize=2

for i in `seq 1 $iterations`
do
  IDIR="${STATSDIR}/itt${i}"
  [ -d "${IDIR}" ] || mkdir "${IDIR}"
  for c in "${configs[@]}"
  do
    source $c

    $SPARK_HOME/bin/spark-submit --master $SPARK_MASTER --deploy-mode client \
      --conf "spark.dynamic.experiments.dbsize=${dbsize}" \
      --conf "spark.dynamic.experiments.statisticsDir=$IDIR" \
      ~/planning-dynamic-spark-supplemental/jars/$JARNAME.jar "$@"
  done
done
