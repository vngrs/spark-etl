organization := "com.vngrs"

name := "spark-etl"

version := "0.0.1-SNAPSHOT"

scalaVersion := versions.scala

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % versions.spark % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % versions.spark % "provided",
  "org.apache.spark" % "spark-sql_2.10" % versions.spark % "provided",

  "com.databricks" % "spark-csv_2.10" % versions.sparkCsv,
  "org.apache.commons" % "commons-csv" % versions.apacheCommonsCsv,

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

// NoNeedForMonad =>
// wartremover bug #106
// Overloading =>
// We use lots of overloading in companion objects,
// also it gives warning for case classes' constructors (apply method)
// Throw =>
// Unfortunately for now, we need use throw expressions for situation
// that has really small chance to occur instead of Try or Either
val excludedWarts = Seq(Wart.NoNeedForMonad, Wart.Overloading, Wart.Throw)

wartremoverErrors ++= Warts.unsafe.filterNot(w => excludedWarts.exists(_.clazz == w.clazz))

wartremoverWarnings ++= Warts.allBut(excludedWarts:_*)

// coverage settings
coverageEnabled := true

coverageMinimum := 80

coverageFailOnMinimum := true

coverageHighlighting := false

// NoSchemaException =>
// It is a very basic Exception class. There is no need to test it since it will work if it can be compiled.
coverageExcludedPackages := "NoSchemaException"

// Only one spark context is allowed at the same time
parallelExecution in Test := false
