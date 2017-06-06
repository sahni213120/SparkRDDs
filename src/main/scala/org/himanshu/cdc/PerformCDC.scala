package scala.org.himanshu.cdc

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.himanshu.helper.{CDCProperties, Constants}

/**
  * Class used to perform CDC Operations between two data frames
  * Created by himanshu on 6/4/2017.
  */
class PerformCDC(cdcDataFrame: DataFrame, masterDataFrame: DataFrame, cdcProperties: CDCProperties) extends CDC {

  /**
    * Performs a full outer join between two given dataframes
    * @return dataframe after join
    */
  @Override
  def join: DataFrame = {

    return cdcDataFrame.as(Constants.CDC_TABLE_ALIAS) join(masterDataFrame.as("master"), col(Constants.CDC_TABLE_ALIAS + "." + cdcProperties.getSourceKeyColumn)
      === col(Constants.MASTER_TABLE_ALIAS + "." + cdcProperties.getTargetKeyCOlumn), Constants.FULL_OUTER_JOIN)
  }

  /**
    * Identifies new records that came in today's load by checking where master key is null.
    * @param joinResults
    * @return data with new records
    */
  @Override
  def getNewRecords(joinResults: DataFrame): DataFrame = {
    //TODO - This has to be made dynamic
    val columnList = cdcProperties.getSourceColumns

    return joinResults.filter(joinResults(Constants.MASTER_TABLE_ALIAS + "." + cdcProperties.getTargetKeyCOlumn).isNull)
      .select(Constants.CDC_TABLE_ALIAS + "." + columnList(0), Constants.CDC_TABLE_ALIAS + "." + columnList(1),
        Constants.CDC_TABLE_ALIAS + "." + columnList(2)).withColumn(Constants.STATUS_COLUMN, lit(Constants.Status.A.toString))
  }

  /**
    * Identify Updated Records
    * @param joinResults
    * @return data with updated active records
    */
  @Override
  def getUpdatedRecordsNew(joinResults: DataFrame): DataFrame = {
    //TODO - This has to be made dynamic
    val columnList = cdcProperties.getSourceColumns

    return joinResults.filter(joinResults(Constants.MASTER_TABLE_ALIAS + "." + cdcProperties.getTargetKeyCOlumn).isNotNull && joinResults(Constants.CDC_TABLE_ALIAS + "." + cdcProperties.getSourceKeyColumn).isNotNull)
      .select(Constants.CDC_TABLE_ALIAS + "." + columnList(0), Constants.CDC_TABLE_ALIAS + "." + columnList(1),
        Constants.CDC_TABLE_ALIAS + "." + columnList(2)).withColumn(Constants.STATUS_COLUMN, lit(Constants.Status.A.toString))
  }

  /**
    * Identify Updated Records
    * @param joinResults
    * @return data with updated expired records
    */
  @Override
  def getUpdatedRecordsOld(joinResults : DataFrame) : DataFrame = {

    //TODO - This has to be made dynamic
    val columnList = cdcProperties.getTargetColumns

    return joinResults.filter(joinResults(Constants.MASTER_TABLE_ALIAS + "." + cdcProperties.getTargetKeyCOlumn).isNotNull && joinResults(Constants.CDC_TABLE_ALIAS + "." + cdcProperties.getSourceKeyColumn).isNotNull)
      .select(Constants.MASTER_TABLE_ALIAS + "." + columnList(0), Constants.MASTER_TABLE_ALIAS + "." + columnList(1),
        Constants.MASTER_TABLE_ALIAS + "." + columnList(2)).withColumn(Constants.STATUS_COLUMN,lit(Constants.Status.I.toString))

  }

  /**
    * Identify records from master which have not changed
    * @param joinResults
    * @return
    */
  @Override
  def getNotChangedRecordsMaster(joinResults : DataFrame) : DataFrame = {

    //TODO - This has to be made dynamic
    val columnList = cdcProperties.getTargetColumns

    return joinResults.where(joinResults(Constants.CDC_TABLE_ALIAS + "." + cdcProperties.getSourceKeyColumn).isNull)
      .select(Constants.MASTER_TABLE_ALIAS + "." + columnList(0), Constants.MASTER_TABLE_ALIAS + "." + columnList(1),
        Constants.MASTER_TABLE_ALIAS + "." + columnList(2),Constants.MASTER_TABLE_ALIAS + "." + columnList(3))
  }

  /**
    * Performs a union of all the data frames to provide final valid data set
    * @param newRecords
    * @param updatedRecordsNew
    * @param updatedRecordsOld
    * @param notChangedRecords
    * @return final data set
    */
  @Override
  def getFinalResults (newRecords : DataFrame, updatedRecordsNew : DataFrame, updatedRecordsOld : DataFrame,
                       notChangedRecords : DataFrame) : DataFrame = {

    return newRecords.union(updatedRecordsNew).union(updatedRecordsOld).union(notChangedRecords).coalesce(1)
  }

  /**
    * Driver method for this class
    * @return Returns final data frame
    */
  @Override
  def run(): DataFrame ={

    val joinResults = join
    val newRecords = getNewRecords(joinResults)
    val updatedRecordsNew = getUpdatedRecordsNew(joinResults)
    val updatedRecordOld = getUpdatedRecordsOld(joinResults)
    val noChangedRecords = getNotChangedRecordsMaster(joinResults)

    return getFinalResults(newRecords,updatedRecordsNew,updatedRecordOld,noChangedRecords)
  }

}
