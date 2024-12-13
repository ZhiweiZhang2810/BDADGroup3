package consumer

import org.apache.log4j.{Level, Logger}
import org.apache.log4j.spi.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types._

object KafkaConsumer {
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.setLevel(Level.WARN)

    val spark = SparkSession.builder
      .appName("Ride Stream Consumer")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate();

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
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

    val (ongoingTripsDf, busiestLocationsDf) = StreamingAnalytics(jsonDf)


    val hudiOptions = Map(
      "hoodie.datasource.write.recordkey.field" -> "location_id",
      "hoodie.datasource.write.precombine.field" -> "event_time",
      "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.upsert.shuffle.parallelism" -> "2",
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.cleaner.policy" -> "KEEP_LATEST_COMMITS",
      "hoodie.keep.min.commits" -> "20",
      "hoodie.keep.max.commits" -> "30",
      "hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
    )


    val hudiTableNameBusiestLocations = "busiest_locations"
    val hudiTablePathBusiestLocations = "file:///path/to/hudi/table/busiest_locations"

    val hudiTableNameOngoingTrips = "ongoing_trips"
    val hudiTablePathOngoingTrips = "file:///path/to/hudi/table/ongoing_trips"

    val hudiOptionsBusiestLocations = commonHudiOptions ++ Map(
      "hoodie.table.name" -> hudiTableNameBusiestLocations,
      "hoodie.datasource.write.recordkey.field" -> "location_id",
      "hoodie.datasource.write.partitionpath.field" -> "event_type"
    )

    val hudiOptionsOngoingTrips = commonHudiOptions ++ Map(
      "hoodie.table.name" -> hudiTableNameOngoingTrips,
      "hoodie.datasource.write.recordkey.field" -> "window",
      "hoodie.datasource.write.partitionpath.field" -> ""
    )

    val hudiSink1: StreamingQuery = busiestLocationsDf.writeStream
      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
        batchDf.write.format("org.apache.hudi")
          .options(hudiOptionsBusiestLocations)
          .mode("append")
          .save(hudiTablePathBusiestLocations)

        println("The busiest location")
        batchDf.orderBy(col("event_count").desc)
          .where(col("event_type") === "pickup")
          .limit(1)
          .show(truncate = false)

        println("The most popular location")
        batchDf.orderBy(col("event_count").desc)
          .where(col("event_type") === "dropoff")
          .limit(1)
          .show(truncate = false)
      }
      .option("checkpointLocation", "file:///tmp/hudi_checkpoint_busiest_locations")
      .start()

    val hudiSink2: StreamingQuery = ongoingTripsDf.writeStream
      .format("org.apache.hudi")
      .options(hudiOptionsOngoingTrips)
      .option("checkpointLocation", "file:///tmp/hudi_checkpoint_ongoing_trips")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start(hudiTablePathOngoingTrips)

    hudiSink1.awaitTermination()
    hudiSink2.awaitTermination()
  }
}