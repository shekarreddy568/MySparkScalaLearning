val sparkVersion = "3.0.0"

lazy val root = (project in file(".")).settings(
  inThisBuild(
    List(
      organization := "com.test.learning",
      scalaVersion := "2.12.12",
      version := "0.1",
      assemblyJarName in assembly := "learning_spark.jar"
    )
  ),
  name := "learning_spark",
  libraryDependencies ++= List(
    //Spark
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql"  % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion,
    "org.apache.spark" %% "spark-avro" % sparkVersion,
    "io.delta"         %% "delta-core" % "0.8.0",
    //Aws
    "org.apache.hadoop" % "hadoop-aws"          % "3.2.0",
    "org.apache.hadoop" % "hadoop-common"       % "3.2.0",
    "com.amazonaws"     % "aws-java-sdk-bundle" % "1.11.874",
    "org.endpoints4s"  %% "akka-http-client"    % "2.0.0",
    // Config and logging
    "com.lambdista" %% "config" % "0.7.1",
    "com.typesafe"   % "config" % "1.2.0",
    // Testing
    "org.scalatest" %% "scalatest"    % "3.2.7" % Test,
    "org.mockito"    % "mockito-core" % "2.7.22",
    "org.specs2"    %% "specs2-core"  % "4.9.3" % Test
  )
)

parallelExecution in ThisBuild := false

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf"              => MergeStrategy.concat
  case x                             => MergeStrategy.first
}
