import sbt._
import sbt.Keys.{concurrentRestrictions, _}
import sbtassembly.AssemblyPlugin.autoImport._
import sbt.ExclusionRule

object CommonSettings {
  val sparkVersion = "2.1.0"

  val sbtSettings = Seq(
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),

    // spark-testing-base requirements
    fork in Test := true,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:MaxMetaspaceSize=2048m", "-XX:+CMSClassUnloadingEnabled"),
    parallelExecution in Test := false
  )

  lazy val commonSettings = baseSettings ++ sbtSettings

  lazy val baseSettings = Seq(
    organization := "nl.tno",
    version := "0.1",
    scalaVersion := "2.11.8",
    libraryDependencies ++= Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "org.clapper" %% "grizzled-slf4j" % "1.3.4"
    ),
    assemblyMergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) =>
        xs match {
          case ("BenchmarkList" :: Nil) | ("CompilerHints" :: Nil) =>
            MergeStrategy.deduplicate
          case _ => MergeStrategy.discard
        }
      case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
      case PathList("org", "apache", xs@_*) => MergeStrategy.first
      case PathList("org", "jboss", xs@_*) => MergeStrategy.first
      case "about.html" => MergeStrategy.rename
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

  lazy val sparkDependencies = Seq(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided" excludeAll (
        ExclusionRule(organization = "org.scala-lang"),
        ExclusionRule(organization = "org.scalacheck"),
        ExclusionRule(organization = "org.scalactic"),
        ExclusionRule(organization = "org.scalatest")
      )
    )
  )

  val testDependencies = Seq(
    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic" % "3.2.0" % "test",
      "org.scalatest" %% "scalatest" % "3.2.0" % "test",
      "org.scalatestplus" %% "mockito-3-3" % "3.2.0.0" % "test",
      "org.mockito" % "mockito-core" % "3.3.3" % "test"
    )
  )
}
