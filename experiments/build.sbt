import CommonSettings._

lazy val experimentBase = ProjectRef(file("."), "experimentBase")

lazy val experimentSettings = commonSettings ++ sparkDependencies

lazy val scenarioComplex = (project in file("scenarios/complex"))
  .settings(experimentSettings)
  .dependsOn(experimentBase)

lazy val scenarioMiddle = (project in file("scenarios/middle"))
  .settings(experimentSettings)
  .dependsOn(experimentBase)

lazy val scenarioSimple = (project in file("scenarios/simple"))
  .settings(experimentSettings)
  .dependsOn(experimentBase)

lazy val noplanOverheadComplex = (project in file("noplanoverhead/complex"))
  .settings(experimentSettings)
  .settings(
    name := "complexNoPlanOverhead",
    mainClass in Compile := Some("rugds.experiments.noplanoverhead.complex.NoPlanOverheadComplex"),
    mainClass in assembly := Some("rugds.experiments.noplanoverhead.complex.NoPlanOverheadComplex")
  )
  .dependsOn(experimentBase, scenarioComplex)

lazy val noplanOverheadMiddle = (project in file("noplanoverhead/middle"))
  .settings(experimentSettings)
  .settings(
    name := "middleNoPlanOverhead",
    mainClass in Compile := Some("rugds.experiments.noplanoverhead.middle.NoPlanOverheadMiddle"),
    mainClass in assembly := Some("rugds.experiments.noplanoverhead.middle.NoPlanOverheadMiddle")
  )
  .dependsOn(experimentBase, scenarioMiddle)

lazy val noplanOverheadSimple = (project in file("noplanoverhead/simple"))
  .settings(experimentSettings)
  .settings(
    name := "simpleNoPlanOverhead",
    mainClass in Compile := Some("rugds.experiments.noplanoverhead.simple.NoPlanOverheadSimple"),
    mainClass in assembly := Some("rugds.experiments.noplanoverhead.simple.NoPlanOverheadSimple")
  )
  .dependsOn(experimentBase, scenarioSimple)


lazy val planningBenefitSettings = experimentSettings ++ Seq(
  packageOptions in assembly ~= { pos =>
    pos.filterNot { po =>
      po.isInstanceOf[Package.MainClass]
    }
  }
)

lazy val planningBenefitComplex = (project in file("planningbenefit/complex"))
  .settings(planningBenefitSettings)
  .settings(
    name := "complexPlanningBenefit"
  )
  .dependsOn(experimentBase, scenarioComplex)

lazy val planningBenefitMiddle = (project in file("planningbenefit/middle"))
  .settings(planningBenefitSettings)
  .settings(
    name := "middlePlanningBenefit"
  )
  .dependsOn(experimentBase, scenarioMiddle)

lazy val planningBenefitSimple = (project in file("planningbenefit/simple"))
  .settings(planningBenefitSettings)
  .settings(
    name := "simplePlanningBenefit"
  )
  .dependsOn(experimentBase, scenarioSimple)


lazy val experimentVariablePipeline = (project in file("variablepipeline"))
  .settings(experimentSettings)
  .settings(
    name := "experiment-variablePipeline",
    mainClass in Compile := Some("rugds.experiments.variablepipeline.ExperimentVariablePipeline"),
    mainClass in assembly := Some("rugds.experiments.variablepipeline.ExperimentVariablePipeline")
  )
  .dependsOn(experimentBase)
