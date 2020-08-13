package net.beon.intern.spark.learning

import org.apache.spark.sql.SparkSession

object People {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark SQL example").master("local")
      .config("spark.hadoop.yarn.resourcemanager.hostname", "hadoop-fra-1")
      .config("spark.hadoop.yarn.resourcemanager.address", "hadoop-fra-1.intern.beon.net:8088")
      .config("hive.metastore.uris","thrift://hadoop-fra-4.intern.beon.net:9083")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.json("hdfs://hadoop-fra-4.intern.beon.net:8020/tmp/data/people.json")

    //df.write.format("json").save("hdfs://hadoop-fra-1.intern.beon.net:8020/tmp/output")

    df.write.saveAsTable("people")
  }

}
