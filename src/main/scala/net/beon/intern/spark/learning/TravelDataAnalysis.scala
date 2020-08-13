package net.beon.intern.spark.learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TravelDataAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Travel Data Analysis").master("local").getOrCreate()
    import spark.implicits._

    val data = spark.sparkContext.textFile("hdfs://hadoop-fra-4.intern.beon.net:8020/tmp/data/TravelData.txt")

    val y = data.map(x => x.split("\t")).map(x => x(2)).toDF("To Location").groupBy("To Location").count().orderBy($"count".desc).take(20)
    y.foreach(println)


    val z = data.map(x => x.split("\t")).map(x => x(1)).toDF("From Location").groupBy("From Location").count().orderBy($"count".desc).take(20)

    z.foreach(println)

    val xx = data.map(x => x.split("\t")).map(x => (x(2),x(3))).toDF("To Location","Product type").filter($"Product type" === 1)
      .groupBy("To Location").count().orderBy(desc("count")).take(20)

    xx.foreach(println)
  }

}
