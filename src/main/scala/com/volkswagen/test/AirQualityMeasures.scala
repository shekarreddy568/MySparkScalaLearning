package com.volkswagen.test

import org.apache.spark.sql.SparkSession

object AirQualityMeasures {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Air Quality Measures")
      .master("local")
      .config("spark.network.timeout","200000s")
      .config("spark.executor.heartbeatInterval", "15000s")
      .config("spark.executor.memory", "5g")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    import spark.implicits._

    val inputData = spark.read.option("multiLine", true).option("mode", "PERMISSIVE").json("/Users/raj.chinthalapelly@mytaxi.com/git-personal/MySparkScalaProject/src/main/resources/air_quality_measures.json")
    inputData.printSchema()

  }

}
