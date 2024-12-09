package hudi

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.{ComplexKeyGenerator, SimpleKeyGenerator}

object HudiWriter {

  def WriteToHudi(jsonDf: org.apache.spark.sql.DataFrame): Unit = {

    val hudiOptions = Map(
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "transactionid",
      DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "country",
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "transactionTime",
      HoodieWriteConfig.TABLE_NAME -> "hoodie_transactions",
      HoodieWriteConfig.OPERATION -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      HoodieWriteConfig.RECORDKEY_FIELD -> "transactionid",
      HoodieWriteConfig.PRECOMBINE_FIELD -> "transactionTime",
      HoodieWriteConfig.PARTITIONPATH_FIELD -> "country",
      HoodieWriteConfig.KEYGENERATOR_CLASS_OPT_KEY -> classOf[SimpleKeyGenerator].getName,
      DataSourceWriteOptions.PATH_OPT_KEY -> "hdfs://namenode:8020/user/hudi/hoodie_transactions" // hdfs path
    )

    // write into hudi
    val hudiQuery = jsonDf
      .writeStream
      .outputMode("append")
      .format("hudi")
      .options(hudiOptions)
      .option("checkpointLocation", "hdfs://namenode:8020/user/hudi/checkpoint") // checkpoint
      .start()

    hudiQuery.awaitTermination()

  }

}
