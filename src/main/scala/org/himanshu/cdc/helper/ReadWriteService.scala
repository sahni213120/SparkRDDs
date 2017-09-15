package scala.org.himanshu.cdc.helper

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by himanshu on 6/4/2017.
  */

@SerialVersionUID(100L)
class ReadWriteService(spark : SparkSession, fileLocation : String, headerString : String, delimiter : String)
  extends java.io.Serializable
{

  def readAndGetDF() : DataFrame = {
    // create the schema from a string, splitting by delimiter
    val headerSchema = StructType(headerString.split(delimiter).map(fieldName => StructField(fieldName, StringType, true)))

    //create input RDD
    val inputRDD = spark.sparkContext.textFile(fileLocation).map(row => row.split(delimiter,-1))
      .map(a => Row.fromSeq(a))

    return spark.sqlContext.createDataFrame(inputRDD , headerSchema)

  }

}
