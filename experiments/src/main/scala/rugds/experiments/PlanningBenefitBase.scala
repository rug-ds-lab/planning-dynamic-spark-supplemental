package rugds.experiments

import org.apache.spark.{SparkConf, SparkContext}

class PlanningBenefitBase extends ExperimentBase {
  val conf: SparkConf = makeConf()
  val sc: SparkContext = makeContext(conf)

  def shutdown(): Unit = {
    sc.stop()
  }

  def getDbSizeMultiplier: Int = {
    getDbSizeMultiplier(conf)
  }

  def getExperimentIterations: Int = {
    conf.get("spark.dynamic.experiments.iterations").toInt
  }
}
