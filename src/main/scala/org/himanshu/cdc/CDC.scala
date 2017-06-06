package scala.org.himanshu.cdc

import org.apache.spark.sql.DataFrame

/**
  * Basic interface to perform CDC operations
  * Created by himanshu on 6/5/2017.
  */
trait CDC {

  def join : DataFrame
  def getNewRecords(joinResults : DataFrame) : DataFrame
  def getUpdatedRecordsNew(joinResults: DataFrame): DataFrame
  def getUpdatedRecordsOld(joinResults: DataFrame): DataFrame
  def getNotChangedRecordsMaster(joinResults : DataFrame) : DataFrame
  def getFinalResults (newRecords : DataFrame, updatedRecordsNew : DataFrame, updatedRecordsOld : DataFrame,
                       notChangedRecords : DataFrame) : DataFrame
  def run(): DataFrame


}
