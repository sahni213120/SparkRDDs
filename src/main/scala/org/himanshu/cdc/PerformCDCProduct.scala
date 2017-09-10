package scala.org.himanshu.cdc

import org.apache.spark.sql.SparkSession
import org.himanshu.helper.CDCProperties

/**
  * Created by himanshu on 9/10/2017.
  */
class PerformCDCProduct(cdcProperties: CDCProperties, spark: SparkSession) extends PerformCDC(cdcProperties, spark) {

  override def createDataFrames: Unit = {

  }

}
