package net.beon.intern.spark.learning

import org.apache.spark.sql.SparkSession

object WeatherDataAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Weather Data Analysis").master("local").getOrCreate()

    import spark.implicits._

    val input = spark.read.option("header", "false").option("inferSchema", "true").csv("hdfs://hadoop-fra-4.intern.beon.net:8020/tmp/data/Temperature.csv")
    input.printSchema()

    input.show()

    val x = input.select("_c0","_c1","_c2","_c3").toDF("Weather Station","Date","Observation Type","Temperature")
        .filter($"Observation Type" === "TMIN").drop("Date").drop("Observation Type")

    val y = x.groupBy("Weather Station").min("Temperature")

    y.show()
  }
}
