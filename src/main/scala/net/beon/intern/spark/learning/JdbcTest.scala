package net.beon.intern.spark.learning

import org.apache.spark.sql.SparkSession

object JdbcTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("jdbc test").master("local").getOrCreate()

    val jdbcDF = spark.read.format("jdbc")
      .option("url","jdbc:postgresql://hadoop-fra-3.intern.beon.net:5432/beon")
      .option("dbtable", "company")
      .option("user","admin").option("password","admin")
      .load()


//    jdbcDF.select("id","name").write.format("jdbc")
//      .option("url","jdbc:postgresql://hadoop-fra-3.intern.beon.net:5432/beon")
//      .option("dbtable", "company_2")
//      .option("user","admin").option("password","admin")
//      .save()

//    val jdbcDF2 = spark.read.format("jdbc")
//      .option("url","jdbc:postgresql://hadoop-fra-3.intern.beon.net:5432/beon")
//      .option("dbtable", "company_2")
//      .option("user","admin").option("password","admin")
//      .load()

    jdbcDF.show()
    jdbcDF.printSchema()
  }

}
