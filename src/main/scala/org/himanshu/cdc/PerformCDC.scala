package scala.org.himanshu.cdc

import java.sql.Date
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
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

      df.cache()

    }
  }

  override def getNewRecords: DataFrame = {

    val max = getMaxId

    print("MAX is " + max)

    val rdd =  spark.sql(cdcProperties.getNewInsertsQuery)
      .withColumn(Constants.STATUS_COLUMN, lit(Constants.Status.Y.toString))
      .withColumn(Constants.EFFCTV_DT, current_date())
      .withColumn(Constants.XPRTN_DT, lit(""))
      .withColumn(Constants.CREATE_TS, current_timestamp())
      .withColumn(Constants.UPDT_TS, current_timestamp())

    rdd.createOrReplaceTempView("test")

    val s= "select row_number() over (order by 1) + " + max + " as unique_id,movieId,title,genres,edm_active_fl,edm_effctv_dt,edm_xprtn_dt,edm_create_ts,edm_updt_ts from test";


    return spark.sql(s)

  }

  override def getUpdatedRecordsNew: DataFrame = {

    return spark.sql(cdcProperties.getUpdatedRecordsInsertsQuery)
      .withColumn(Constants.STATUS_COLUMN, lit(Constants.Status.Y.toString))
      .withColumn(Constants.EFFCTV_DT, current_date())
      .withColumn(Constants.XPRTN_DT, lit(""))
      .withColumn(Constants.CREATE_TS, current_timestamp())
      .withColumn(Constants.UPDT_TS, current_timestamp())
  }


  override def getUpdatedRecordsOld: DataFrame = {
    return spark.sql(cdcProperties.getUpdatedRecordsUpdatesQuery)

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

    finalOutput.write.format("com.databricks.spark.csv").save(outputLocation)
    //finalOutput.write.parquet(outputLocation)

  }

  def getMaxId: String = {
    return spark.sql("select max(unique_id) from movies_master").head().getString(0)
  }

}

