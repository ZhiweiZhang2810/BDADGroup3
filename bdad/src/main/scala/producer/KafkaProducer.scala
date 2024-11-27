package producer

import org.apache.spark.sql._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;

object KafkaProducer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Transaction Producer")
      .master("local[*]")
      .getOrCreate();

    val schema = StructType(Array(
      StructField("userid", StringType, true),
      StructField("transactionid", StringType, true),
      StructField("transactionTime", TimestampType, true),
      StructField("itemcode", StringType, true),
      StructField("itemdescription", StringType, true),
      StructField("numberofitempurchased", IntegerType, true),
      StructField("costperitem", DoubleType, true),
      StructField("country", StringType, true)
    ));

    val transactionDF = spark.read
      .schema(schema)
      .csv("/user/qz2166_nyu_edu/transaction_data.csv");

    val kvDF = transactionDF
      .withColumn("key", col("userid"))
      .withColumn("value", to_json(struct(
        col("transactionid"),
        col("transactionTime"),
        col("itemcode"),
        col("itemdescription"),
        col("numberofitempurchased"),
        col("costperitem"),
        col("country")
      ))).selectExpr("key", "value");

    val splitWeight = Array.fill(100)(1.0);

    val splitDFs = kvDF.randomSplit(splitWeight, 42);
    
    for (splitDF <- splitDFs) {
      splitDF.write
      .format("kafka")
      .option("kafka.bootstrap.servers", "nyu-dataproc-w-0:9092")
      .option("topic", "bdad-g3")
      .save;

      Thread.sleep(1000);
    }

    spark.stop();
  }
}