ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.3"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val root = (project in file("."))
  .settings(
    name := "MRLogsProcessor"
  )

val logbackVersion = "1.3.0-alpha10"
val sfl4sVersion = "2.0.0-alpha5"
val typesafeConfigVersion = "1.4.1"
val apacheCommonIOVersion = "2.11.0"
val scalacticVersion = "3.2.9"
val generexVersion = "1.0.2"
val hadoopVersion = "3.3.4"

resolvers += Resolver.jcenterRepo

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "ch.qos.logback" % "logback-core" % logbackVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "org.slf4j" % "slf4j-api" % sfl4sVersion,
  "com.typesafe" % "config" % typesafeConfigVersion,
  "commons-io" % "commons-io" % apacheCommonIOVersion,
  "org.scalactic" %% "scalactic" % scalacticVersion,
  "org.scalatest" %% "scalatest" % scalacticVersion % Test,
  "org.scalatest" %% "scalatest-featurespec" % scalacticVersion % Test,
  "com.typesafe" % "config" % typesafeConfigVersion,
  "com.github.mifmif" % "generex" % generexVersion
)
