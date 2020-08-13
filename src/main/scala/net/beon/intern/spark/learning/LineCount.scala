package net.beon.intern.spark.learning


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LineCount {

    def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder().appName("first program").master("yarn-client")
        .config("spark.hadoop.yarn.resourcemanager.hostname", "hadoop-fra-1")
        .config("spark.hadoop.yarn.resourcemanager.address", "hadoop-fra-1.intern.beon.net:8088")
        .getOrCreate()

      import spark.implicits._

      val data= spark.read.textFile("hdfs://hadoop-fra-4.intern.beon.net:8020/tmp/data/test.txt")
      val linesWithA = data.map(x => x.contains("a")).count()

      println(s"lines count: $linesWithA")

    }
  }

