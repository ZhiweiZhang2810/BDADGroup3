from pyspark.sql import SparkSession
from pyspark.sql.streaming import DataStreamReader

def write_to_hudi(df, table_name, record_key, precombine_field, hudi_path, operation="upsert"):
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.precombine.field": precombine_field,
        "hoodie.datasource.write.operation": operation,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.hive_sync.enable": "true",
        "hoodie.datasource.write.hive_sync.database": "default",
        "hoodie.datasource.write.hive_sync.table": table_name,
        "hoodie.datasource.write.hive_sync.partition_fields": "",
        "hoodie.datasource.write.hive_sync.mode": "hms",
        "path": hudi_path
    }

    df.write.format("hudi").options(**hudi_options).mode("append").save()

def create_hudi_table(spark, table_name, hudi_path):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} USING hudi LOCATION '{hudi_path}'")

def update_hudi_table(df, table_name, record_key, precombine_field, hudi_path):
    write_to_hudi(df, table_name, record_key, precombine_field, hudi_path, operation="upsert")

def insert_into_hudi_table(df, table_name, record_key, precombine_field, hudi_path):
    write_to_hudi(df, table_name, record_key, precombine_field, hudi_path, operation="insert")

spark = SparkSession.builder \
    .appName("KafkaToHudiPipeline") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .getOrCreate()

kafka_bootstrap_servers = "nyu-dataproc-w-0:9092"
kafka_topic = "bdad-g3"

data_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

schema = StructType() \
    .add("id", StringType()) \
    .add("name", StringType()) \
    .add("timestamp", StringType()) \
    .add("value", IntegerType())

deserialized_stream = data_stream.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

def process_and_write_to_hudi(batch_df, batch_id):
    create_hudi_table(spark, "hudi_kafka_table", "/user/xs2534_nyu_edu")
    update_hudi_table(
        df=batch_df,
        table_name="hudi_kafka_table",
        record_key="id",
        precombine_field="timestamp",
        hudi_path="/user/xs2534_nyu_edu"
    )

query = deserialized_stream.writeStream \
    .foreachBatch(process_and_write_to_hudi) \
    .option("checkpointLocation", "/user/xs2534_nyu_edu/checkpoint")
    .start()

query.awaitTermination()