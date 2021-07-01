package rugds.experiments

import grizzled.slf4j.Logging
import org.apache.spark.dynamic.planning.DynamicPlanner

abstract class DynamicPipeline extends Logging {
  def initializeVariationPoints(): Unit
  def pipeline(): Int
}
