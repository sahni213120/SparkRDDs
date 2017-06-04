package org.himanshu.sparksql

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SparkSession}

/**
  * @author himanshu
  *
  *         Joining two files using Spark SQL example
  *
  */
object JoinSparkSQL {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().appName("Joining 2 files using Spark SQL").master("local[2]") getOrCreate()

    import spark.implicits._

    //Read text file and create person data frame
    val person = spark.sparkContext.textFile(args(0),2).map { x => x.split(",") }
      .map(x => Person(x(0).trim().toInt, x(1), x(2), x(3))).toDF()

    //Read text file and create address data frame  
    val address = spark.sparkContext.textFile(args(1), 2).map { x => x.split(",") }
      .map { x => Address(x(0).trim().toInt, x(1)) }.toDF()

      
    //create person table
    person.createOrReplaceTempView("person")
    //create address table
    address.createOrReplaceTempView("address")

    //Join person and address tables
    val join = spark.sql("select p.id, p.firstName, p.lastName, p.country, a.address from person p " +
      "inner join address a on (p.id = a.id)")

      
    println("partitions are ",join.rdd.partitions.size)  
    //Show join output on console  
    join.show()

    //Count person by ID
    val countByID = spark.sql("select id, count(*) as ct from person group by id")
    //Show output
    countByID.show()

    FileUtils.deleteDirectory(new File("C:/Users/himanshu/git/SparkRDDs/target/output/"));

    join.rdd.coalesce(1).saveAsTextFile("C:\\Users\\himanshu\\git\\SparkRDDs\\target\\output\\")

    spark.close();

  }

  /**
    * Define schema of the input file
    */
  case class Person(id: Int, firstName: String, lastName: String, country: String)

  /**
    * Define schema of the input file
    */
  case class Address(id: Int, address: String)

}

