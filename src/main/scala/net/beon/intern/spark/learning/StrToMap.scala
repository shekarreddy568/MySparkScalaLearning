package net.beon.intern.spark.learning

import org.apache.spark.sql.SparkSession

object StrToMap {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark SQL example").master("local").getOrCreate()
    import spark.implicits._

    def convert(str : String)  = {
      str.drop(1).dropRight(1).split(", ").map(_.split(": ")).map { case Array(k, v) => k -> v }.toMap
    }

    val data = List("{name: \"John\", age: 31, city: \"New York\"}")

    val df = spark.sparkContext.parallelize(data).toDF()

    df.select("value").map(line => line.toString()).map(line => convert(line)).show(1,false)
  }

}
