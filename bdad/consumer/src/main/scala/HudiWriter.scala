package consumer

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_NAME}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs

object HudiWriter {

  def WriteToHudi(jsonDf: org.apache.spark.sql.DataFrame): Unit = {

    val tableName = "hudi_trips_cow"
    val basePath = "hdfs://tmp/hudi_trips_cow"

    jsonDf.write.format("hudi").
      options(getQuickstartWriteConfigs)
      .option(PRECOMBINE_FIELD.key(), "ts")
      .option(RECORDKEY_FIELD.key(), "uuid")
      .option(PARTITIONPATH_FIELD.key(), "partitionpath")
      .option(TABLE_NAME.key(), tableName)
      .mode("append")
      .save(basePath)

  }

}
