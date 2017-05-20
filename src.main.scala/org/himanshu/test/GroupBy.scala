package org.himanshu.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author himanshu
 * Read a file. Group by ID and get count of objects by ID.
 * Write results into a file
 *
 */
object GroupBy {
  
  def main (args : Array[String]) {
    
    val conf = new SparkConf().setAppName("Group By Example").setMaster("local")
    
    val sc = new SparkContext(conf)
    
    //Read a text file
    val fileRDD = sc.textFile("src/main/resources/InputFile.txt")
    
    //Modify fileRDD to a key value RDD
    val keyValueRDD = fileRDD.map { x => (x.split("|")) }.map { x => (x(0),1) }
    
    //Coung number of occurences by ID
    val groupByRDD = keyValueRDD.reduceByKey((x,y) => (x+y))
    
    groupByRDD.foreach(println)
    
    //Change the format as required and write to an output file
    groupByRDD.map(f => f._1 + "|" + f._2)saveAsTextFile("target/output")
    
  }
  
}