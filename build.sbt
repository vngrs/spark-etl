organization := "com.vngrs"

name := "spark-etl"

version := "0.0.1-SNAPSHOT"

scalaVersion := versions.scala

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % versions.spark % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % versions.spark % "provided",
  "org.apache.spark" % "spark-sql_2.10" % versions.spark % "provided",

  "org.scalactic" %% "scalactic" % versions.scalatest,
  "org.scalatest" %% "scalatest" % versions.scalatest % "test"
)
  // removing sbt warnings
  .map(_.excludeAll(
  ExclusionRule(organization = "com.google.code.findbugs"),
  ExclusionRule(organization = "xmlenc"),
  ExclusionRule(organization = "commons-beanutils"),
  ExclusionRule(organization = "stax")
))

dependencyOverrides ++= Set(
  "org.scala-lang" % "scala-compiler" % versions.scala,
  "org.scala-lang" % "scala-library" % versions.scala,
  "org.scala-lang" % "scala-reflect" % versions.scala,

  "org.apache.commons" % "commons-lang3" % "3.3.2",

  "org.slf4j" % "slf4j-api" % "1.7.10",

  "org.scalamacros" % "quasiquotes_2.10" % "2.0.1"
)

wartremoverErrors ++= Warts.unsafe

// NoNeedForMonad =>
// wartremover bug #106
// Overloading =>
// We use lots of overloading in companion objects,
// also it gives warning for case classes' constructors (apply method)
wartremoverWarnings ++= Warts.allBut(Wart.NoNeedForMonad, Wart.Overloading)

// coverage settings
coverageEnabled := true

coverageMinimum := 80

coverageFailOnMinimum := true

coverageHighlighting := false

// Only one spark context is allowed at the same time
parallelExecution in Test := false
