package producer

import org.apache.spark.sql._;
import org.apache.spark.sql.functions._;

object KafkaProducer {
  val DATE = "2024-9-03";

  def splitDropoffPickup(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
    val pickupDF = df.select(
      lit("pickup").alias("event_type"),
      col("pickup_datetime").alias("event_time"),
      col("PULocationID").alias("location_id")
    );

    val dropoffDF = df.select(
      lit("dropoff").alias("event_type"),
      col("dropoff_datetime").alias("event_time"),
      col("DOLocationID").alias("location_id")
    )

    pickupDF.union(dropoffDF)
  }

  def writeToKafka(df: org.apache.spark.sql.DataFrame): Unit = {
    df.selectExpr("CAST(location_id AS STRING) AS key", "to_json(struct(*)) AS value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("topic", "bdad-g3")
      .save
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Ride Strean Producer")
      .master("local[*]")
      .getOrCreate()

    val homeDir = System.getProperty("user.home")
    val df = spark.read.parquet(s"$homeDir/fhvhv_tripdata_2024-09.parquet")
    val dayDF = splitDropoffPickup(
      df.filter(to_date(col("dropoff_datetime")) === DATE && to_date(col("pickup_datetime")) === DATE))
      .withColumn("uuid", monotonically_increasing_id())
      .sort(asc("event_time"))

    writeToKafka(dayDF.filter(hour(col("event_time")) < 20));
    for (ihour <- 20 to 23) {
      val hourDF = dayDF.filter(hour(col("event_time")) === ihour)
      for (iminute <- 0 to 59) {
        val minuteDF = hourDF.filter(minute(col("event_time")) === iminute)
        writeToKafka(minuteDF.filter(second(col("event_time")) >= 0 && second(col("event_time")) < 30))
        Thread.sleep(5000)
        writeToKafka(minuteDF.filter(second(col("event_time")) >= 30))
        Thread.sleep(5000)
      }
    }

    spark.stop();
  }
}