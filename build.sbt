import sbt.ExclusionRule
import sbtassembly.AssemblyPlugin.autoImport.assemblyShadeRules
import CommonSettings._

name := "planning-dynamic-spark-supplemental"

lazy val experimentBase = (project in file("experiments"))
  .settings(commonSettings)
  .settings(sparkDependencies)
  .settings(
    libraryDependencies ++= Seq(
      "nl.rug" %% "dynamic-spark-core" % "0.0.1-SNAPSHOT2",
      "nl.rug" %% "dynamic-spark-planning" % "0.0.1-SNAPSHOT2"
    )
  )
