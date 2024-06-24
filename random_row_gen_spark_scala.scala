import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.util.Random


object DpmsDevRandomRowGenerator {

    def main(args: Array[String]): Unit = {
        val WRITE_PATH = args(0)
        val totalRows = args(1).toLong
        val numPartitions = args(2).toInt
        val write_format = args(3)
        val with_nulls = args(4)

        val chunkSize = (totalRows/numPartitions).toInt
        
        implicit val spark = SparkSession.builder()
          .appName("DpmsDevRandomRowGenerator")
          .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
          .enableHiveSupport().getOrCreate();
        implicit val sqlContext: SQLContext = spark.sqlContext
        
        val schema = StructType(List(
            StructField("v1", StringType, nullable = true),
            StructField("v2", StringType, nullable = true),
            StructField("v3", StringType, nullable = true),
            StructField("v4", StringType, nullable = true),
            StructField("v5", StringType, nullable = true),
            StructField("v6", IntegerType, nullable = true),
            StructField("v7", StringType, nullable = true),
            StructField("v8", StringType, nullable = true),
            StructField("v9", StringType, nullable = true),
            StructField("v10", StringType, nullable = true),
            StructField("v11", StringType, nullable = true),
            StructField("v12", StringType, nullable = true),
            StructField("v13", StringType, nullable = true),
            StructField("v14", StringType, nullable = true),
            StructField("v15", StringType, nullable = true)            
        ))

        val rdd = if (with_nulls == "true") spark.sparkContext.parallelize(0 until totalRows.toInt, numPartitions).map(dummyGeneratorWithNulls)
                  else spark.sparkContext.parallelize(0 until totalRows.toInt, numPartitions).map(dummyGenerator)

        val df = spark.createDataFrame(rdd, schema)

        write_format match {
                case "csv" => df.write.mode("overwrite").csv(WRITE_PATH)
                case "orc" => df.write.mode("overwrite").orc(WRITE_PATH)
                case "parquet" => df.write.mode("overwrite").parquet(WRITE_PATH)
            }
    }

    def dummyGenerator(l: Int): Row = {
        val rnd = Random
        val v1 = (10000000 + rnd.nextInt(10000000) + 1).toString
        val v2 = (rnd.nextInt(172) + 1).toString
        val v3 = (rnd.nextInt(969) + 1).toString
        val v4 = (rnd.nextInt(157) + 1).toString
        val v5 = (rnd.nextInt(2) + 1).toString
        val v6 = rnd.nextInt(11).toInt
        val v7 = rnd.nextInt(1000).toString
        val v8 = rnd.nextInt(1000).toString
        val v9 = rnd.nextInt(1000).toString
        val v10 = rnd.nextInt(1000).toString
        val v11 = rnd.nextInt(1000).toString
        val v12 = rnd.nextInt(1000).toString
        val v13 = rnd.nextInt(1000).toString
        val v14 = rnd.nextInt(1000).toString
        val v15 = rnd.nextInt(1000).toString

        Row(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15)
    }

    def dummyGeneratorWithNulls(l: Int): Row = {
        val rnd = Random
        val v1 = if (rnd.nextInt(10) == 0) null else (10000000 + rnd.nextInt(10000000) + 1).toString
        val v2 = if (rnd.nextInt(10) == 0) null else (rnd.nextInt(172) + 1).toString
        val v3 = if (rnd.nextInt(10) == 0) null else (rnd.nextInt(969) + 1).toString
        val v4 = if (rnd.nextInt(10) == 0) null else (rnd.nextInt(157) + 1).toString
        val v5 = if (rnd.nextInt(10) == 0) null else (rnd.nextInt(2) + 1).toString
        val v6 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(11).toInt
        val v7 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
        val v8 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
        val v9 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
        val v10 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
        val v11 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
        val v12 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
        val v13 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
        val v14 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString
        val v15 = if (rnd.nextInt(10) == 0) null else rnd.nextInt(1000).toString

        Row(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15)
    }
}
