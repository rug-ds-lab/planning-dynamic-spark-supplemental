package rugds.experiments

import grizzled.slf4j.Logging
import org.apache.spark.dynamic.planning.{DynamicModule, DynamicPlanner}

abstract class PlannedPipeline extends Logging {
  def defaultModules: Seq[DynamicModule[_,_]]
  def initializeVariationPoints(planner: DynamicPlanner): Unit
  def pipeline(planner: DynamicPlanner): Int
}
