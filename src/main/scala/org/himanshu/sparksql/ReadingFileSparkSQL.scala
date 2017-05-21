package org.himanshu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author himanshu
 * Reading and querying a file using spark sql
 * 
 */
object ReadingFileSparkSQL { 
  
  def main(args: Array[String]) {
    
    
  
    //Initialize Spark Configuration
    val conf = new SparkConf().setAppName("Reading file example using spark SQL").setMaster("local")

    //Initialize Spark Context
    val sc = new SparkContext(conf)

    //Initialize Spark Context
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    //Create data frame by reading the input file
    val person = sc.textFile(args(0)).map { _.split(",") }.
      map(p =>
        Person(p(0).trim.toInt, p(1), p(2), p(3).trim())).toDF()

    //show data in data frame
    person.show()
    //show schema
    person.printSchema()

    //Register temporary table
    person.registerTempTable("people")

    //Query firstName where country is India
    val india = sqlContext.sql("SELECT firstName from people where country = 'IN'")

    //write output to a csv file
    india.write.format("com.databricks.spark.csv").save("target/output")

  }

/**
 * Define schema of the input file  
 */
case class Person(id: Int, firstName: String, lastName: String, country: String)

}