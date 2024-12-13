package consumer

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, concat_ws, count, when, window}
import org.apache.spark.sql.types.TimestampType

object StreamingAnalytics {
  def apply(df: DataFrame)(implicit spark: SparkSession): (DataFrame, DataFrame) = {
    val homeDir = System.getProperty("user.home")
    val locationDf = spark.read.option("header", "true").csv(s"$homeDir/taxi_zone_lookup.csv")
      .select(col("LocationID").alias("location_id"), concat_ws(",", col("Zone"), col("Borough")).alias("Location")).cache()

    // ongoingTripsDf
    val ongoingTripsDf = df.withColumn("event_time", col("event_time").cast("timestamp"))
      .withWatermark("event_time", "5 minutes") // 添加 watermark 以进行状态管理
      .groupBy(window(col("event_time"), "5 minutes", "1 minute"))
      .agg(
        (functions.sum(when(col("event_type") === "pickup", 1).otherwise(0)) -
          functions.sum(when(col("event_type") === "dropoff", 1).otherwise(0)))
          .alias("ongoing_trips")
      )

    // busiestLocationsDf
    val busiestLocationsDf = df.withColumn("event_time", col("event_time").cast("timestamp"))
      .withWatermark("event_time", "5 minutes")
      .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("location_id"),
        col("event_type")
      )
      .agg(count("*").alias("event_count"))
      .join(locationDf, "location_id")

    (ongoingTripsDf, busiestLocationsDf)
  }
}