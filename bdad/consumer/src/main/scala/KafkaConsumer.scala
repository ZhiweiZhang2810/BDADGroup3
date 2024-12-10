package consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types._

object KafkaConsumer {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Ride Stream Consumer")
      .master("local[*]")
      .getOrCreate();

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "nyu-dataproc-w-0:9092")
      .option("subscribe", "bdad-g3")
      .option("startingOffsets","earliest")
      .load()

    val schema = StructType(Array(
      StructField("event_type",StringType,false),
      StructField("event_time",TimestampNTZType,true),
      StructField("location_id",IntegerType,true)))

    val jsonDf = df.selectExpr("CAST(value AS STRING) AS value")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
      .drop(col("data"))

    StreamingAnalytics(jsonDf, spark);

    val query = jsonDf
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
