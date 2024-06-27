package com.samsung.b2b.analytics.dpms

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random


object VerblobDevRandomRowGenerator {

  def main(args: Array[String]): Unit = {
    /*
    val WRITE_PATH = args(0)
    val totalRows = args(1).toLong
    val numPartitions = args(2).toInt
    val write_format = args(3)
    val with_nulls = args(4)
     */

    val WRITE_PATH = s"file:///Users/s1.dutta/Documents/git_repos/analytics-spark-dpms-daily-processing-localversion/results/"
    val totalRows = "3000000000".toLong
    val numPartitions = "500".toInt
    val write_format = "orc"
    val with_nulls = "true"

    println(s"WRITE_PATH : $WRITE_PATH")
    println(s"totalRows : $totalRows")
    println(s"numPartitions : $numPartitions")
    println(s"write_format : $write_format")
    println(s"with_nulls : $with_nulls")

    /*
    val chunkSize = (totalRows/numPartitions).toInt
    */

    implicit val spark = SparkSession.builder()
      .appName("VerblobDevRandomRowGenerator")
      .master("local[*]")
      .config("spark.driver.memory.Overhead", "1024")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.driver.memory", "512m")
      .config("spark.driver.cores", "1")
      .config("spark.executor.instances", "1")
      .config("spark.executor.memory", "6g")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .enableHiveSupport().getOrCreate();
    implicit val sqlContext: SQLContext = spark.sqlContext

    val schema = StructType(List(
      StructField("imei", StringType, nullable = true),
      StructField("verblobid", StringType, nullable = true),
      StructField("model_name", StringType, nullable = true),
      StructField("android_version", StringType, nullable = true),
      StructField("latest_version", StringType, nullable = true),
      StructField("v6", StringType, nullable = true),
      StructField("v7", StringType, nullable = true),
      StructField("v8", StringType, nullable = true),
      StructField("v9", StringType, nullable = true),
      StructField("v10", StringType, nullable = true),
      StructField("v11", StringType, nullable = true),
      StructField("v12", StringType, nullable = true),
      StructField("v13", StringType, nullable = true),
      StructField("v14", StringType, nullable = true),
      StructField("v15", StringType, nullable = true),
      StructField("v16", StringType, nullable = true),
      StructField("v17", StringType, nullable = true),
      StructField("v18", StringType, nullable = true),
      StructField("v19", StringType, nullable = true),
      StructField("v20", StringType, nullable = true),
      StructField("v21", StringType, nullable = true),
      StructField("v22", StringType, nullable = true),
      StructField("v23", StringType, nullable = true),
      StructField("v24", StringType, nullable = true)
    ))

    val rdd = if (with_nulls == "true") spark.sparkContext.parallelize(1L until totalRows, numPartitions).map(dummyGeneratorWithNulls)
                                   else spark.sparkContext.parallelize(1L until totalRows, numPartitions).map(dummyGenerator)

    val df = spark.createDataFrame(rdd, schema)

    write_format match {
      case "csv" => df.write.mode("overwrite").csv(WRITE_PATH)
      case "orc" => df.write.mode("overwrite").orc(WRITE_PATH)
      case "parquet" => df.write.mode("overwrite").parquet(WRITE_PATH)
    }
  }

  def dummyGenerator(l: Long): Row = {
    val rnd = Random
    val v1 = (100000000 + rnd.nextInt(450000000) + 1).toString
    val v2 = (rnd.nextInt(2780) + 1).toString
    val v3 = (rnd.nextInt(1000) + 1).toString
    val v4 = (rnd.nextInt(10000) + 1).toString
    val v5 = (rnd.nextInt(2) + 1).toString
    val v6 = rnd.nextInt(11).toString
    val v7 = rnd.nextInt(1000).toString
    val v8 = rnd.nextInt(1000).toString
    val v9 = rnd.nextInt(1000).toString
    val v10 = rnd.nextInt(1000).toString
    val v11 = rnd.nextInt(1000).toString
    val v12 = rnd.nextInt(1000).toString
    val v13 = rnd.nextInt(1000).toString
    val v14 = rnd.nextInt(1000).toString
    val v15 = rnd.nextInt(1000).toString
    val v16 = rnd.nextInt(1000).toString
    val v17 = rnd.nextInt(1000).toString
    val v18 = rnd.nextInt(1000).toString
    val v19 = rnd.nextInt(1000).toString
    val v20 = rnd.nextInt(1000).toString
    val v21 = rnd.nextInt(1000).toString
    val v22 = rnd.nextInt(1000).toString
    val v23 = rnd.nextInt(1000).toString
    val v24 = rnd.nextInt(1000).toString

    Row(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, v23, v24)
  }

  def dummyGeneratorWithNulls(l: Long): Row = {
    val rnd = Random
    val v1 = if (rnd.nextInt(10) == 0) null else (100000000 + rnd.nextInt(450000000) + 1).toString
    val v2 = if (rnd.nextInt(10) == 0) null else (rnd.nextInt(2780) + 1).toString
    val v3 = if (rnd.nextInt(10) == 0) null else (rnd.nextInt(1000) + 1).toString
    val v4 = if (rnd.nextInt(10) == 0) null else (rnd.nextInt(10000) + 1).toString
    val v5 = if (rnd.nextInt(10) == 0) null else (rnd.nextInt(2) + 1).toString
    val v6 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(11).toString
    val v7 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v8 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v9 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v10 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v11 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v12 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v13 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v14 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v15 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v16 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v17 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v18 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v19 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v20 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v21 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v22 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v23 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
    val v24 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString

    Row(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, v23, v24)
  }
}

