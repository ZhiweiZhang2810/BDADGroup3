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
      .option("startingOffsets", "earliest")
      .load()

    val schema = StructType(Array(
      StructField("event_type", StringType, nullable = false),
      StructField("event_time", TimestampNTZType, nullable = true),
      StructField("location_id", IntegerType, nullable = true)))

    val jsonDf = df.selectExpr("CAST(value AS STRING) AS value")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
      .drop(col("data"))

    val qPair = StreamingAnalytics(jsonDf)(spark)

    // Write to Hudi
    //    val dataSink = writeToHudi()(spark)
    //    dataSink.awaitTermination()
    // WriteToHudi(jsonDF)

    qPair._1.awaitTermination()
    qPair._2.awaitTermination()
  }
}
