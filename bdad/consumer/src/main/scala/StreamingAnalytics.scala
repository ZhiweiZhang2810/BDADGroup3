package consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, count, window}
import org.apache.spark.sql.streaming.StreamingQuery

object StreamingAnalytics {
    def apply(df: org.apache.spark.sql.DataFrame)(implicit spark: SparkSession): StreamingQuery = {
        val homeDir = System.getProperty("user.home")
        val locationDf = spark.read.option("header", "true").csv(s"$homeDir/taxi_zone_lookup.csv")
            .select(col("LocationID").alias("location_id"), concat_ws(",", col("Zone"), col("Borough")).alias("Location")).cache;

        // zone with the most pickups in the last five minutes
        val windowedCounts = df.withColumn("event_time", col("event_time").cast("timestamp"))
            .filter(col("event_type") === "pickup")
            .withWatermark("event_time", "5 minute")
            .groupBy(window(col("event_time"), "5 minute", "1 minute"), col("location_id"))
            .agg(count("*").alias("num_pickups"))
            .orderBy(col("num_pickups").desc)
            .join(locationDf, "location_id")
            .limit(1)

        windowedCounts.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .start()
    }
}