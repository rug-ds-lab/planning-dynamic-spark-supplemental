#!/bin/bash
STATSDIR="~/planning-dynamic-spark-supplemental/stats"
[ -d "${STATSDIR}" ] || mkdir "${STATSDIR}"

OUTDIR="${STATSDIR}/$(date '+%d-%m-%YT%Hh%m')"
[ -d "${OUTDIR}" ] || mkdir "${OUTDIR}"

trap '
  trap - INT # restore default INT handler
  kill -s INT "$$"
' INT

configs=("experiment-variable-pipeline.sh")
outerItt=16
smallItt=8
innerItt=30

for c in "${configs[@]}"
  do
    source $c

    for i in `seq 1 $outerItt`
    do
    # Vary steps
    steps=$i
    echo "Experiment: length, steps=${steps}"
    $SPARK_HOME/bin/spark-submit --master $SPARK_MASTER --deploy-mode client \
      --conf "spark.dynamic.experiments.test=length" \
      --conf "spark.dynamic.experiments.steps=${steps}" \
      --conf "spark.dynamic.experiments.alternatives=1" \
      --conf "spark.dynamic.experiments.joins=0" \
      --conf "spark.dynamic.experiments.iterations=${innerItt}" \
      --conf "spark.dynamic.experiments.statisticsDir=$OUTDIR" \
      ~/planning-dynamic-spark-supplemental/jars/$JARNAME.jar "$@"

    # Vary alternatives
    # was with steps=10
    alternatives=$i
    echo "Experiment: alternatives, alternatives=${alternatives}"
    $SPARK_HOME/bin/spark-submit --master $SPARK_MASTER --deploy-mode client \
      --conf "spark.dynamic.experiments.test=alternatives" \
      --conf "spark.dynamic.experiments.steps=4" \
      --conf "spark.dynamic.experiments.alternatives=${alternatives}" \
      --conf "spark.dynamic.experiments.joins=0" \
      --conf "spark.dynamic.experiments.iterations=${innerItt}" \
      --conf "spark.dynamic.experiments.statisticsDir=$OUTDIR" \
      ~/planning-dynamic-spark-supplemental/jars/$JARNAME.jar "$@"

    # Vary joins, type allequal
    joins=$i
    echo "Experiment: join-allequal, joins=${joins}"
    $SPARK_HOME/bin/spark-submit --master $SPARK_MASTER --deploy-mode client \
      --conf "spark.dynamic.experiments.test=join-allequal" \
      --conf "spark.dynamic.experiments.steps=1" \
      --conf "spark.dynamic.experiments.alternatives=1" \
      --conf "spark.dynamic.experiments.joins=${joins}" \
      --conf "spark.dynamic.experiments.iterations=${innerItt}" \
      --conf "spark.dynamic.experiments.statisticsDir=$OUTDIR" \
      ~/planning-dynamic-spark-supplemental/jars/$JARNAME.jar "$@"
  done

  for i in `seq 1 $smallItt`
    do
    # Vary joins, type either
    joins=$i
    echo "Experiment: join-either, joins=${joins}"
    $SPARK_HOME/bin/spark-submit --master $SPARK_MASTER --deploy-mode client \
      --conf "spark.dynamic.experiments.test=join-either" \
      --conf "spark.dynamic.experiments.steps=1" \
      --conf "spark.dynamic.experiments.alternatives=1" \
      --conf "spark.dynamic.experiments.joins=${joins}" \
      --conf "spark.dynamic.experiments.iterations=${innerItt}" \
      --conf "spark.dynamic.experiments.statisticsDir=$OUTDIR" \
      ~/planning-dynamic-spark-supplemental/jars/$JARNAME.jar "$@"
  done
done
