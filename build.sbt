name := "SparkML_Homework"
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,

  "org.apache.spark" % "spark-mllib_2.12" % sparkVersion % "provided",
  "org.apache.spark" % "spark-streaming_2.12" % sparkVersion % "provided",
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % sparkVersion,

  "com.typesafe" % "config" % "1.4.2",
)

assembly / assemblyMergeStrategy := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x: String if x.contains("UnusedStubClass.class") => MergeStrategy.first
  case _ => MergeStrategy.first
}