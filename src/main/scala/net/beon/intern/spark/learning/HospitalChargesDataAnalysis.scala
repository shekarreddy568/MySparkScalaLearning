package net.beon.intern.spark.learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HospitalChargesDataAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Hospital Charges Data Analysis").master("local")
      .config("spark.sql.shuffle.partitions",5)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val inputData = spark.read.option("inferSchema","true").option("header","true")
      .csv("hdfs://hadoop-fra-4.intern.beon.net:8020/tmp/data/inpatientCharges.csv")

    inputData.printSchema()

    //val totalAvgCoveredChargesPerState = inputData.groupBy("ProviderState").avg("AverageCoveredCharges")

    val dischargesPerState = inputData.groupBy("ProviderState","DRGDefinition").sum("TotalDischarges")
      .withColumnRenamed("sum(TotalDischarges)","total").orderBy($"total".desc).cache()

    //dischargesPerState.write.format("parquet").mode("overwrite").save("hdfs://hadoop-fra-4.intern.beon.net:8020/tmp/result")
    dischargesPerState.write.mode("overwrite").saveAsTable("dischargesPerState")

    val jspnData = dischargesPerState.toJSON

//    jspnData.select("value")
//      .write.format("kafka")
//      .option("kafka.bootstrap.servers", "hadoop-fra-5.intern.beon.net:9092")
//      .option("topic","dischargesPerState")
//      .save()




  }

}
