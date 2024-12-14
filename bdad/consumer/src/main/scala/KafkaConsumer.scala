package consumer


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, from_json,concat_ws, count, when, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.TimestampType



object KafkaConsumer {
  val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.setLevel(Level.WARN)

    implicit val spark: SparkSession = SparkSession.builder
      .appName("Ride Stream Consumer")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate();

    import spark.implicits._

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




    // Hudi option
    val commonHudiOptions = Map(
      "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.datasource.write.operation" -> "upsert",
      "hoodie.cleaner.policy" -> "KEEP_LATEST_COMMITS",
      "hoodie.keep.min.commits" -> "20",
      "hoodie.keep.max.commits" -> "30",
      "hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"
    )


    // busiestLocationsDf
    val hudiOptionsBusiestLocations = commonHudiOptions ++ Map(
      "hoodie.table.name" -> "busiest_locations",
      "hoodie.datasource.write.precombine.field" -> "event_time",
      "hoodie.datasource.write.recordkey.field" -> "location_id",
      "hoodie.datasource.write.partitionpath.field" -> "event_type"
    )

    // ongoingTripsDf
    val hudiOptionsOngoingTrips = commonHudiOptions ++ Map(
      "hoodie.table.name" -> "ongoing_trips",
      "hoodie.datasource.write.recordkey.field" -> "window",
      "hoodie.datasource.write.partitionpath.field" -> ""
    )
    val homeDir = System.getProperty("user.home");
    val locationDf = spark.read.option("header", "true").csv(s"$homeDir/taxi_zone_lookup.csv")
      .select(col("LocationID").alias("location_id"), concat_ws(",", col("Zone"), col("Borough")).alias("Location")).cache()
    val busiestLocationsDf  = jsonDf.withColumn("event_time", col("event_time").cast("timestamp"))
      .withWatermark("event_time", "5 minute")
      .groupBy(window(col("event_time"), "5 minute", "1 minute"), col("location_id"), col("event_type"), col("event_time"))
      .agg(count("*").alias("event_count"))
      .join(locationDf, "location_id")
      .writeStream.format("org.apache.hudi").
      options(hudiOptionsBusiestLocations).
      outputMode("append").
      option("path", "file:///home/xs2534_nyu_edu/hudi_table/busiest_locations").
      option("checkpointLocation", "file:///home/xs2534_nyu_edu/hudi_table/hudi_checkpoint_busiest_locations").
      trigger(Trigger.Once()).
      start()



    val ongoingTripsDf  = df.withColumn("event_time", col("event_time").cast("timestamp"))
      .agg(
        functions.sum(when(col("event_type") === "pickup", 1).otherwise(0)).alias("num_pickups") -
          functions.sum(when(col("event_type") === "dropoff", 1).otherwise(0)).alias("num_dropoffs")
      ).as("ongoing_trips")
      .writeStream
      .trigger(Trigger.ProcessingTime("1 minute"))
      .format("org.apache.hudi").
      options(hudiOptionsOngoingTrips).
      outputMode("append").
      option("path", "file:///home/xs2534_nyu_edu/hudi_table/ongoing_trips").
      option("checkpointLocation", "file:///home/xs2534_nyu_edu/hudi_table/hudi_checkpoint_ongoing_trips").
      trigger(Trigger.Once()).
      start()






    ongoingTripsDf.awaitTermination()
    busiestLocationsDf.awaitTermination()
  }
}
