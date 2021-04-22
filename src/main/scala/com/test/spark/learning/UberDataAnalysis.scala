package com.test.spark.learning

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object UberDataAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Uber Data Analysis")
      .master("local")
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.orc.impl", "native")
      .getOrCreate()
    import spark.implicits._

    val inputData = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("hdfs://hadoop-fra-4.intern.beon.net:8020/tmp/data/uber")

    inputData.show()
    inputData.printSchema()

    val daysWithMoreTrips = inputData.select(
      $"dispatching_base_number",
      $"trips",
      dayofweek(to_date($"date", "MM/dd/yyyy")).as("day_of_week")
    )

    val daysWithMoreTrips2 =
      daysWithMoreTrips.groupBy($"dispatching_base_number", $"day_of_week").agg(sum("trips").as("total"))

    val ordered = daysWithMoreTrips2
      .withColumn("num", dense_rank().over(Window.partitionBy($"dispatching_base_number").orderBy($"total".desc)))
      .filter($"num" === 1)
      .drop("num")
      .orderBy($"total".desc)

    ordered.show()

    def getDay(c: Int): String =
      c match {
        case 1 => "Monday"
        case 2 => "Tuesday"
        case 3 => "Wednesday"
        case 4 => "Thursday"
        case 5 => "Friday"
        case 6 => "Saturday"
        case 7 => "Sunday"
      }

    val dayUDF = udf((i: Int) => getDay(i))

    val result = ordered.withColumn("dayOfWeek", dayUDF($"day_of_week"))

    result.write.format("orc").mode("overwrite").save("hdfs://hadoop-fra-4.intern.beon.net:8020/tmp/result1")

    val resultJson = result.toJSON

    resultJson.show()

    resultJson
      .select("value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop-fra-5.intern.beon.net:9092")
      .option("topic", "UberData")
      .save()
  }

}
