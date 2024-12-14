package consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, monotonically_increasing_id}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Ride Stream Consumer")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .master("local[*]")
      .getOrCreate();

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("subscribe", "bdad-g3")
      .option("startingOffsets", "earliest")
      .load()

    val schema = StructType(Array(
      StructField("uuid", StringType, nullable = false),
      StructField("event_type", StringType, nullable = false),
      StructField("event_time", TimestampType, nullable = true),
      StructField("location_id", IntegerType, nullable = true)))

    val jsonDf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
      .drop(col("data"))

    // Original data streamed into Hudi backed w/ Hive Metastore
    val checkpointPath = "file:///tmp/hudi/checkpoints/"
    val dataPath = "file:///tmp/hudi/data/"
    val streamingTableName = "tlc_trips"

    val streamDataToHudi = jsonDf.writeStream.format("hudi")
      .option("hoodie.datasource.write.precombine.field", "timestamp")
      .option("hoodie.datasource.write.recordkey.field", "uuid")
      .option("hoodie.datasource.write.partitionpath.field", "key")
      .option("hoodie.table.name", streamingTableName)
      .option("hoodie.datasource.meta.sync.enable", "true")
      .option("hoodie.datasource.hive_sync.mode", "hms")
      .option("hoodie.datasource.hive_sync.metastore.uris", "thrift://localhost:9083")
      .option("hoodie.datasource.write.streaming.retry.count", 6)
      .option("path", dataPath)
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime("1 minute"))
      .outputMode("append")
      .start()

    val qPair = StreamingAnalytics(jsonDf)(spark)

    streamDataToHudi.awaitTermination()
    qPair._1.awaitTermination()
    qPair._2.awaitTermination()
  }
}
