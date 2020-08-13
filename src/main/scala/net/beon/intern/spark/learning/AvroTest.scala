package net.beon.intern.spark.learning

import org.apache.spark.sql.SparkSession

object AvroTest {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Avro Test").master("local").getOrCreate()
    val df = spark.read.format("avro").load("hdfs://hadoop-fra-4.intern.beon.net:8020/tmp/data/doctors.avro")

    df.select("number","first_name").write.format("avro").save("hdfs://hadoop-fra-1.intern.beon.net:8020/tmp/out_avro")

  }

}
