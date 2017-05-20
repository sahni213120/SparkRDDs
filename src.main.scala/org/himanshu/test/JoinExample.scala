package org.himanshu.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author himanshu
 * Example to join two files in Spark
 **
 */
object JoinExample {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Join Example").setMaster("local")
    val sc = new SparkContext(conf)

    val inputNamesRDD = sc.textFile("src/main/resources/InputFile.txt")
    val addressRDD = sc.textFile("src/main/resources/Address.txt")

  }

}