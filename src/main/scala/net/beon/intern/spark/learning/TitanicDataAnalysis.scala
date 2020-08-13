package net.beon.intern.spark.learning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TitanicDataAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Titanic Data Analysis").getOrCreate()

    import spark.implicits._

    val data = spark.read.textFile("hdfs://hadoop-fra-4.intern.beon.net:8020/tmp/data/TitanicData.txt")

    val df = data.map(y => y.split(",")).map(x => (x(1).asInstanceOf[String],x(4).asInstanceOf[String],x(5).asInstanceOf[String])).toDF("Survived","Sex","Age")

    val res = df.filter($"Age" !== "").filter($"Survived" !== "").filter($"Survived" === "1").select($"Sex",$"Age".cast("Int"))
      .groupBy("Sex").avg("Age")

    val df2 = data.map(x => (x(1),x(4),x(5),x(2).asInstanceOf[String])).toDF("Survived","Sex","Age","Pclass")

    val res2 =df2.filter($"Age" !== "").filter($"Survived" !== "")
      .select($"Survived",$"Sex",$"Age",$"Pclass".cast("String")).groupBy("Survived","Sex","Age","Pclass").count()
    res2.show()
  }
}
