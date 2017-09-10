package scala.org.himanshu.cdc

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.himanshu.helper.{CDCProperties, Constants}

import scala.org.himanshu.cdc.helper.ReadWriteService

/**
  * Class used to perform CDC Operations between two data frames
  * Created by himanshu on 6/4/2017.
  */
class PerformCDC(cdcProperties: CDCProperties, spark: SparkSession) extends CDC {

  override def createDataFrames: Unit = {
    val tables = cdcProperties.getTable

    for (table <- tables) {
      val df = new ReadWriteService(spark, table.getLocation, table.getHeaderString, ",").readAndGetDF()
      df.createOrReplaceTempView(table.getTableName)

    }
  }

  override def getNewRecords: DataFrame = {

    return spark.sql(cdcProperties.getNewInsertsQuery)
      .withColumn(Constants.STATUS_COLUMN, lit(Constants.Status.A.toString))

  }

  override def getUpdatedRecordsNew: DataFrame = {

    return spark.sql(cdcProperties.getUpdatedRecordsInsertsQuery)
      .withColumn(Constants.STATUS_COLUMN, lit(Constants.Status.A.toString))
  }


  override def getUpdatedRecordsOld: DataFrame = {
    return spark.sql(cdcProperties.getUpdatedRecordsUpdatesQuery)
      .withColumn(Constants.STATUS_COLUMN, lit(Constants.Status.I.toString))

  }

  override def getNotChangedRecordsMaster: DataFrame = {
    return spark.sql(cdcProperties.getUnchangedRecordsQuery)

  }

  override def getFinalResults(newRecords: DataFrame, updatedRecordsNew: DataFrame, updatedRecordsOld: DataFrame,
                               notChangedRecords: DataFrame): DataFrame = {
    return newRecords.union(updatedRecordsNew).union(updatedRecordsOld).union(notChangedRecords).coalesce(1)
  }


  /**
    * Driver method for this class
    *
    * @return Returns final data frame
    */
  override def run(outputLocation: String) = {

    createDataFrames
    val newRecords = getNewRecords
    val updatedRecordsNew = getUpdatedRecordsNew
    val updatedRecordOld = getUpdatedRecordsOld
    val noChangedRecords = getNotChangedRecordsMaster

    val finalOutput = getFinalResults(newRecords, updatedRecordsNew, updatedRecordOld, noChangedRecords)

    finalOutput.write.parquet(outputLocation)

  }

}
