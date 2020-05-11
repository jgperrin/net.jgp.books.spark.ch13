name := "SparkInAction2-Chapter13"

version := "1.0.0"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.0-preview2"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.delta"         %% "delta-core"   % "0.6.0"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
