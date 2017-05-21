package org.himanshu.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.himanshu.helper.PropertiesHelper

/**
 * @author himanshu
 * Read a file. Group by ID and get count of objects by ID.
 * Write results into a file
 *
 */
object GroupBy {

  def main(args: Array[String]) {
    
    val property = new PropertiesHelper(args, 2);
    
    val filePath = property.getValue("inputFile");
    
    val outputPath = property.getValue("outputPath");

    val conf = new SparkConf().setAppName("Group By Example").setMaster("local")

    val sc = new SparkContext(conf)

    //Read a text file
    val fileRDD = sc.textFile(filePath)

    //Modify fileRDD to a key value RDD
    val keyValueRDD = fileRDD.map { x => (x.split("|")) }.map { x => (x(0), 1) }

    //Coung number of occurences by ID
    val groupByRDD = keyValueRDD.reduceByKey((x, y) => (x + y))

    groupByRDD.foreach(println)

    //Change the format as required and write to an output file
    groupByRDD.map(f => f._1 + "|" + f._2).saveAsTextFile(outputPath)

  }

}