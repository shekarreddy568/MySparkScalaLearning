name := "MySparkScalaLearning"

version := "0.1"

scalaVersion := "2.12.8"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-yarn
libraryDependencies += "org.apache.spark" %% "spark-yarn" % "2.4.5"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.1.1"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.1.1"


////// Jackson Dependencies //////////
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.8"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.8"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.8"

// https://mvnrepository.com/artifact/net.jpountz.lz4/lz4
libraryDependencies += "net.jpountz.lz4" % "lz4" % "1.3.0"

lazy val excludeJars = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

