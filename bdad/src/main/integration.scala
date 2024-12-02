import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StringType, IntegerType}
import org.apache.spark.sql.functions.{from_json, col}

object KafkaToHudiPipeline {

  def writeToHudi(df: org.apache.spark.sql.DataFrame,
                  tableName: String,
                  recordKey: String,
                  precombineField: String,
                  hudiPath: String,
                  operation: String = "upsert"): Unit = {

    val hudiOptions = Map(
      "hoodie.table.name" -> tableName,
      "hoodie.datasource.write.recordkey.field" -> recordKey,
      "hoodie.datasource.write.precombine.field" -> precombineField,
      "hoodie.datasource.write.operation" -> operation,
      "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
      "hoodie.datasource.write.hive_sync.enable" -> "true",
      "hoodie.datasource.write.hive_sync.database" -> "default",
      "hoodie.datasource.write.hive_sync.table" -> tableName,
      "hoodie.datasource.write.hive_sync.partition_fields" -> "",
      "hoodie.datasource.write.hive_sync.mode" -> "hms",
      "path" -> hudiPath
    )

    df.write.format("hudi").options(hudiOptions).mode("append").save()
  }

  def createHudiTable(spark: SparkSession, tableName: String, hudiPath: String): Unit = {
    spark.sql(s"CREATE TABLE IF NOT EXISTS $tableName USING hudi LOCATION '$hudiPath'")
  }

  def updateHudiTable(df: org.apache.spark.sql.DataFrame,
                      tableName: String,
                      recordKey: String,
                      precombineField: String,
                      hudiPath: String): Unit = {
    writeToHudi(df, tableName, recordKey, precombineField, hudiPath, "upsert")
  }

  def insertIntoHudiTable(df: org.apache.spark.sql.DataFrame,
                          tableName: String,
                          recordKey: String,
                          precombineField: String,
                          hudiPath: String): Unit = {
    writeToHudi(df, tableName, recordKey, precombineField, hudiPath, "insert")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("KafkaToHudiPipeline")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .getOrCreate()

    val kafkaBootstrapServers = "nyu-dataproc-w-0:9092"
    val kafkaTopic = "bdad-g3"

    val dataStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .load()

    val schema = new StructType()
      .add("id", StringType)
      .add("name", StringType)
      .add("timestamp", StringType)
      .add("value", IntegerType)

    val deserializedStream = dataStream.selectExpr("CAST(value AS STRING) as json_data")
      .select(from_json(col("json_data"), schema).alias("data"))
      .select("data.*")

    def processAndWriteToHudi(batchDF: org.apache.spark.sql.DataFrame, batchId: Long): Unit = {
      createHudiTable(spark, "hudi_kafka_table", "/user/xs2534_nyu_edu")
      updateHudiTable(
        df = batchDF,
        tableName = "hudi_kafka_table",
        recordKey = "id",
        precombineField = "timestamp",
        hudiPath = "/user/xs2534_nyu_edu"
      )
    }

    val query = deserializedStream.writeStream
      .foreachBatch(processAndWriteToHudi _)
      .option("checkpointLocation", "/user/xs2534_nyu_edu/checkpoint")
      .start()
    query.awaitTermination()
  }
}
