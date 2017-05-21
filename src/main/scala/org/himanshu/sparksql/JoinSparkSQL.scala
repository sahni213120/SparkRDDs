package org.himanshu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * @author himanshu
 *
 * Joining two files using Spark SQL example
 *
 */
object JoinSparkSQL {

  def main(args: Array[String]) {

    //Set Spark conf
    val conf = new SparkConf().setAppName("Joining 2 files using Spark SQL").setMaster("local")
    //Set Spark Context
    val sc = new SparkContext(conf)

    //Initialize Sql Context
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //Read text file and create person data frame
    val person = sc.textFile(args(0)).map { x => x.split(",") }
      .map(x => Person(x(0).trim().toInt, x(1), x(2), x(3))).toDF()

    //Read text file and create address data frame  
    val address = sc.textFile(args(1)).map { x => x.split(",") }
      .map { x => Address(x(0).trim().toInt, x(1)) }.toDF()

    //create person table
    person.registerTempTable("person")
    //create address table
    address.registerTempTable("address")

    //Join person and address tables
    val join = sqlContext.sql("select p.id, p.firstName, p.lastName, p.country, a.address from person p " +
      "inner join address a on (p.id = a.id)")

    //Show join output on console  
    join.show()

    //Count person by ID
    val countByID = sqlContext.sql("select id, count(*) as ct from person group by id")
    //Show output
    countByID.show()

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

