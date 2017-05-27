package scala.org.himanshu.rdd

import java.text.SimpleDateFormat

import com.google.protobuf.TextFormat.ParseException
import org.apache.spark.{SparkConf, SparkContext}
import org.himanshu.helper.DateUtility

/**
  * Created by himanshu on 5/27/2017.
  */
object ValidateInput {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Filter Input Data").setMaster("local")

    val sc = new SparkContext(conf)

    val inputRDD = sc.textFile("src/main/resources/InputFile-new.txt")

    val newInputRDD = inputRDD.map(line => line.split(","))

    val errorRows = newInputRDD.filter(p => DateUtility.isDateValid(p(4), "yyyyMMdd"))

    print("Count is " + errorRows.count())

    errorRows.saveAsTextFile("target/output")


  }


}
