package org.himanshu.rdd

import java.text.SimpleDateFormat

import com.google.protobuf.TextFormat.ParseException
import org.apache.spark.{SparkConf, SparkContext}
import org.himanshu.helper.DateUtility
import org.apache.commons.io.FileUtils
import java.io.File

/**
  * Created by himanshu on 5/27/2017.
  */
object ValidateInput {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Filter Input Data").setMaster("local")

    val sc = new SparkContext(conf)

    val inputRDD = sc.textFile("src/main/resources/InputFile-new.txt")

    val newInputRDD = inputRDD.map(line => line.split(","))

    val errorRows = newInputRDD.filter(p => !DateUtility.isDateValid(p(4), "yyyyMMdd"))

    print("Error Count is " + errorRows.count())

    FileUtils.deleteDirectory(new File("C:\\Users\\himanshu\\test"))
    
    errorRows.map( x => (x(0) + "," + x(1) + "," + x(2) + "," + x(3) + "," + x(4))).saveAsTextFile("C:\\Users\\himanshu\\test")
    

  }


}
