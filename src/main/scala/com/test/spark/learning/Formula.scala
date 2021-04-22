package com.test.spark.learning

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object Formula {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Avro Test").master("local").getOrCreate()
    import spark.implicits._

    val df = spark.sparkContext
      .parallelize(
        Seq(
          (1, "A*(B+C)", "A", 5),
          (1, "A*(B+C)", "B", 6),
          (1, "A*(B+C)", "C", 7),
          (2, "A/B", "A", 12),
          (2, "A/B", "B", 6)
        )
      )
      .toDF("ID", "Formula", "step", "Value")

    val df2 = df.groupBy($"ID", $"Formula").agg(collect_list(map($"step", $"Value")) as "map")

    val joinMap = udf { values: Seq[Map[String, Int]] => values.flatten.toMap }

    val df3 = df2.withColumn("map", joinMap(col("map")))

    def calculate(df: DataFrame): Column = {
      when($"ID" === 1, $"map" ("A") * ($"map" ("B") + $"map" ("C")))
        .otherwise(when($"ID" === 2, $"map" ("A") / $"map" ("B")))
    }

    df3.withColumn("value", lit(calculate(df3))).drop("map").show()

  }

}
