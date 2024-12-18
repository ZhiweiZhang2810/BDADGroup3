package consumer

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, concat_ws, count, when, window}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object StreamingAnalytics {
  def apply(df: org.apache.spark.sql.DataFrame)(implicit spark: SparkSession): (StreamingQuery, StreamingQuery) = {
    val homeDir = System.getProperty("user.home")
    val locationDf = spark.read
      .option("header", "true")
      .csv(s"$homeDir/taxi_zone_lookup.csv")
      .select(col("LocationID").alias("location_id"), concat_ws(",", col("Zone"), col("Borough")).alias("location"))

    // the number of pickups in the last five minutes
    val q2Count = df.withColumn("event_time", col("event_time").cast("timestamp"))
      .agg(
        functions.sum(when(col("event_type") === "pickup", 1).otherwise(0)).alias("num_pickups") -
          functions.sum(when(col("event_type") === "dropoff", 1).otherwise(0)).alias("num_dropoffs")
      ).as("ongoing_trips")
      .writeStream
      .trigger(Trigger.ProcessingTime("1 minute"))
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .option("checkpointLocation", "./cp/q4")
      .outputMode("complete")
      .start()

    // Zone with the most pickups in the last five minutes
    val q1Count = df.withColumn("event_time", col("event_time").cast("timestamp"))
      .withWatermark("event_time", "5 minute")
      .groupBy(window(col("event_time"), "5 minute", "1 minute"), col("location_id"), col("event_type"))
      .agg(count("*").alias("event_count"))
      .join(locationDf, "location_id")
      .writeStream
      .outputMode("append")
      .foreachBatch((bdf: org.apache.spark.sql.DataFrame, batchId: Long) => {
        println("The busiest location")
        bdf.orderBy(col("event_count").desc)
          .where(col("event_type") === "pickup")
          .drop("location_id")
          .limit(1)
          .show()

        println("The most popular location")
        bdf.orderBy(col("event_count").desc)
          .where(col("event_type") === "dropoff")
          .drop("location_id")
          .limit(1)
          .show()
      })
      .option("checkpointLocation", "./cp/q3")
      .option("truncate", "false")
      .start()

    (q1Count, q2Count)
  }
}