package net.beon.intern.spark.learning

import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Travel Data Analysis").master("local").getOrCreate()
    import spark.implicits._

    val url = "https://ipinfo.io/countries"
    val res = scala.io.Source.fromURL(url)
    println(res)
  }

}
