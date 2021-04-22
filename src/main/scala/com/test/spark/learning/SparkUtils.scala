package com.test.spark.learning

import com.amazonaws.auth.profile.{ProfileCredentialsProvider, ProfilesConfigFile}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.amazonaws.auth.BasicSessionCredentials
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest
import io.delta.tables._
import org.apache.log4j.Logger

object SparkUtils {

  val log = Logger.getLogger(getClass.getName)

  def createSparkSession(isLocal: Boolean = false): SparkSession = {
    if(isLocal) {
      SparkSession
        .builder()
        .appName("spark_util_test")
        .master("local[*]")
        .config(
          "spark.hadoop.fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
        )
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", getAwsCredentials._1)
        .config("spark.hadoop.fs.s3a.secret.key", getAwsCredentials._2)
        .config("spark.hadoop.fs.s3a.session.token", getAwsCredentials._3)
        .getOrCreate()

    } else {
      SparkSession
        .builder()
        .config("hive.metastore.uris", "")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    }
  }

  /**
   * Load data from remote/local location
   * @param spark: Spark session
   * @param format: Data format in remote/local location. Default format is AVRO
   * @param location: Remote local location
   * @return: DataFrame
   */
  def read(spark: SparkSession, format: String = "avro", location: String): DataFrame = {
    spark.read
      .option("recursiveFileLookup", "true")
      .format(format)
      .load(location)
  }

  /**
   * Load delta table from remote/local location
   * @param spark: Spark session
   * @param location: Remote location
   * @return: DeltaTable
   */
  def readDeltaTable(spark: SparkSession, location: String): DeltaTable = {
    DeltaTable.forPath(spark, location)
  }

  /**
   * Write DataFrame into local/remote location
   * @param df: data
   * @param mode: Write mode. Default mode is 'overwrite'
   * @param format: Data format in remote/local location. Default format is AVRO
   * @param noFilesInPartition: Number of files, default is 10
   * @param partitionByColumns: List of partition columns
   * @param dstLocation: Remote/local location
   */
  def write(
      df: DataFrame,
      mode: String = "overwrite",
      format: String = "parquet",
      noFilesInPartition: Int = 10,
      partitionByColumns: List[String] = List(),
      dstLocation: String
  ): Unit = {
    if(partitionByColumns.isEmpty) {
      df.repartition(noFilesInPartition)
        .write
        .mode(mode)
        .format(format)
        .save(dstLocation)
    } else {
      df.repartition(noFilesInPartition)
        .write
        .mode(mode)
        .partitionBy(partitionByColumns.mkString(","))
        .format(format)
        .save(dstLocation)
    }
  }

  def getAwsCredentials: (String, String, String) = {

    val stsClient = AWSSecurityTokenServiceClientBuilder
      .standard()
      .withCredentials(
        new ProfileCredentialsProvider(
          new ProfilesConfigFile("/Users/raj.chinthalapelly@mytaxi.com/.aws/credentials"),
          "elisabeth"
        )
      )
      .withRegion("eu-west-1")
      .build()

    val roleRequest = new AssumeRoleRequest()
      .withRoleArn("arn:aws:iam::639112474826:role/data-engineer")
      .withRoleSessionName("localSession")

    val roleResponse       = stsClient.assumeRole(roleRequest)
    val sessionCredentials = roleResponse.getCredentials

    val awsCredentials = new BasicSessionCredentials(
      sessionCredentials.getAccessKeyId,
      sessionCredentials.getSecretAccessKey,
      sessionCredentials.getSessionToken
    )

    val awsAccessKey    = awsCredentials.getAWSAccessKeyId
    val awsSecretKey    = awsCredentials.getAWSSecretKey
    val awsSessionToken = awsCredentials.getSessionToken

    log.info(s"awsAccessKey: $awsAccessKey, awsSecretKey: $awsSecretKey, awsSessionToken: $awsSessionToken")

    (awsAccessKey, awsSecretKey, awsSessionToken)

  }

}
