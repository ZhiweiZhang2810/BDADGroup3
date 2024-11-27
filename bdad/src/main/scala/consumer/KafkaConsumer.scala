package consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Transaction Consumer")
      .master("local[*]")
      .getOrCreate();

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "nyu-dataproc-w-0:9092")
      .option("subscribe", "bdad-g3")
      .load()

    val schema = StructType(Array(
      StructField("userid", StringType, true),
      StructField("transactionid", StringType, true),
      StructField("transactionTime", TimestampType, true),
      StructField("itemcode", StringType, true),
      StructField("itemdescription", StringType, true),
      StructField("numberofitempurchased", IntegerType, true),
      StructField("costperitem", DoubleType, true),
      StructField("country", StringType, true)
    ));

    val jsonDf = df.selectExpr("CAST(value AS STRING) AS value")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")

    val query = jsonDf.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
