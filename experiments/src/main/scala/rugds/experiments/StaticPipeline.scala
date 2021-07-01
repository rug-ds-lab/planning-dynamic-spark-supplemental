package rugds.experiments

import grizzled.slf4j.Logging

abstract class StaticPipeline extends Logging {
  def pipeline(): Int
}
