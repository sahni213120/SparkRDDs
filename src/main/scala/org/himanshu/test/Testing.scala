package scala.org.himanshu.test

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by himanshu on 8/5/2017.
  */
object Testing {

  def main (args : Array[String]): Unit =
  {

    val spark = SparkSession.builder().appName("Test Job").master("local").getOrCreate()

   /* val flightData = spark.read.option("inferSchema", "false").option("header","true")
      .csv("src/main/resources/TestFile.txt")*/

    val flightData = spark.sparkContext.textFile("src/main/resources/TestFile.txt").
      map(row => row.split(","))

    val newRDD = flightData.map(x =>validateRow(x))

    newRDD.collect().foreach(println)

    val validRDD = newRDD.filter(_._1.isEmpty).map(_._2)

    validRDD.collect().foreach(println)

    val rejectRDD = newRDD.filter(!_._1.isEmpty)

    rejectRDD.collect().foreach(println)

    val headerSchema = StructType("error_cd,data".split(",").map(fieldName => StructField(fieldName, StringType, true)))


    val errorDF =  spark.sqlContext.createDataFrame(rejectRDD)

    


    /*val validData = flightData.filter(validateRow(_))

    val invalidData = flightData.except(validData)
    */

 //   flightData.take(4).foreach{println}


  //  val rejectedRDD = flightData.filter(validateRow(_))

//rejectedRDD.take(4).foreach(println)

    /*
val newRDD = flightData.withColumn("errorCode", validateRowUDF())

    newRDD.take(4).foreach {println}
*/


    //spark.sqlContext.udf.register("myValidationFunction", validateRow(_))

  //  val newRDD = flightData.withColumn("errorCode", validateRow(StructType(flightData.columns.map(flightData(_)) : _*)))

   // val newRDD = flightData.map(x => validateRow(x))

    //val validRdd = newRDD.filter(line => line.split(",")(0).isEmpty)

    //validRdd.take(4).foreach(println)

    /*flightData.createOrReplaceTempView("FLIGHT_DATA")

    val count = spark.sql("select count(*) from flight_data")
*/
  //  println("Valid Records are " + validData.collect().foreach(print(_)) + "\n")

  }





  def validateRow (row : Array[String]) : (String, Array[String])  = {


    var errorCode = "";

    if (row.size > 3)
      {
        errorCode = "123";
      }

    if (row(0).isEmpty)
      {
        errorCode = errorCode + "|" + "456"
      }

    return (errorCode ,  row)

  }

}
