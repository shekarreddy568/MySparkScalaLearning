package net.beon.intern.spark.learning


import java.nio.file.{Files, Paths}

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.avro._

object KafkaTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Kafka Test").master("local").getOrCreate()
    import spark.implicits._

    //val schemaFile = new String(Files.readAllBytes(Paths.get("src/main/resources/schema.avsc")))
    val schemaFile = "/src/main/resources/schema.avsc"

    val df = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "hadoop-fra-5.intern.beon.net:9092")
      .option("subscribe", "test1")
      .load()

    df.printSchema()
    df.show(2,false)
    val xxx = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    xxx.show()

    df.select("key","value")
      .write.format("kafka")
      .option("kafka.bootstrap.servers", "hadoop-fra-5.intern.beon.net:9092")
      .option("topic","test2")
      .save()

    val df2 = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "hadoop-fra-5.intern.beon.net:9092")
      .option("subscribe", "test2")
      .load()

    df2.printSchema()
    df2.show(5,false)
    //df.select(from_avro($"value", "schemaFile").as("user")).show()

    //df.show()
    //val xxx = df.schema
    //println(xxx)
  }

}
