package consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, count, window}
import org.apache.spark.sql.streaming.StreamingQuery

object StreamingAnalytics {
    def apply(df: org.apache.spark.sql.DataFrame)(implicit spark: SparkSession): StreamingQuery = {
        val homeDir = System.getProperty("user.home")
        val locationDf = spark.read.option("header", "true").csv(s"$homeDir/taxi_zone_lookup.csv")
            .select(col("LocationID").alias("location_id"), concat_ws(",", col("Zone"), col("Borough")).alias("Location")).cache;

        // the number of pickups in the last five minutes
        val windowedSums = jsonDf.withColumn("event_time", col("event_time").cast("timestamp"))
            .withWatermark("event_time", "5 minute")
            .groupBy(window(col("event_time"), "5 minute", "1 minute"))
            .agg(
                sum(when(col("event_type") === "pickup", 1).otherwise(0)).alias("num_pickups"),
                sum(when(col("event_type") === "dropoff", 1).otherwise(0)).alias("num_dropoffs")
            )
            .withColumn("change_in_num_ongoing_trips", col("num_pickups") - col("num_dropoffs"));

        // zone with the most pickups in the last five minutes
        val windowedCounts = df.withColumn("event_time", col("event_time").cast("timestamp"))
            .filter(col("event_type") === "pickup")
            .withWatermark("event_time", "5 minute")
            .groupBy(window(col("event_time"), "5 minute", "1 minute"), col("location_id"))
            .agg(count("*").alias("num_pickups"))
            .orderBy(col("num_pickups").desc)
            .join(locationDf, "location_id")
            .limit(1);

        val sumQuery = windowedSums.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .start();

        val countQuery = windowedCounts.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .start();
    }
}