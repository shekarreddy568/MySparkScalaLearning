package com.test.spark.learning

import org.apache.spark.sql.SparkSession

object StructuredStreamingWordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Streaming Word Count").master("local").getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "hadoop-fra-4.intern.beon.net")
      .option("port", 9999)
      .load()

    val words     = lines.as[String].flatMap(_.split(" "))
    val wordCount = words.groupBy("value").count()

    val query = wordCount.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
