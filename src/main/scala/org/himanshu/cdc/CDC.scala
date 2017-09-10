package scala.org.himanshu.cdc

import org.apache.spark.sql.DataFrame

/**
  * Basic interface to perform CDC operations
  * Created by himanshu on 6/5/2017.
  */
trait CDC {

  def createDataFrames
  def getNewRecords : DataFrame
  def getUpdatedRecordsNew : DataFrame
  def getUpdatedRecordsOld : DataFrame
  def getNotChangedRecordsMaster : DataFrame
  def getFinalResults (newRecords : DataFrame, updatedRecordsNew : DataFrame, updatedRecordsOld : DataFrame,
                       notChangedRecords : DataFrame) : DataFrame
  def run(outputLocation : String)


}
