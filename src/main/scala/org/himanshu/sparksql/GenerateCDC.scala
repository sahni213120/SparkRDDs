package scala.org.himanshu.sparksql

import org.apache.spark.sql.{functions, SparkSession}
import org.apache.spark.sql.functions._



/**
  * Created by himanshu on 6/2/2017.
  */
object GenerateCDC {


  def main(args: Array[String]): Unit =
  {
    val spark = SparkSession.builder().appName("CDC Example").master("local").getOrCreate()

    //Read the master file
    val master = spark.sqlContext.read.format("com.databricks.spark.csv").option("header","true")
      .option("inferSchema","true").load("src/main/resources/movies_master.csv")

    //Read the cdc file
    val cdc = spark.sqlContext.read.format("com.databricks.spark.csv").option("header","true")
      .option("inferSchema","true").load("src/main/resources/movies_cdc.csv")

    //Create Temp View for Master
    master.createTempView("master")

    //Create Temp View for CDC
    cdc.createTempView("cdc")

    //Full outer join between master and cdc
    val joinResults = spark.sql("SELECT C.movieid as cdc_movieid,c.title as cdc_title, c.genres as cdc_genres," +
      " M.movieid as mst_movieid, m.title as mst_title, m.genres as mst_genres,m.status as mst_status FROM CDC C FULL OUTER JOIN MASTER M ON (C.MOVIEID = M.MOVIEID)")

    //New Records are those where master id is null
    val newRecords = joinResults.where("mst_movieid is null").select("cdc_movieid", "cdc_title", "cdc_genres").withColumn("status",lit("A"))

    //Updated Records are those where both ids are not null
    val updatedRecords = joinResults.where("mst_movieid is not null and cdc_movieid is not null")

    //Create new active records for each updated record
    val updatedRecordsNew = updatedRecords.select("cdc_movieid", "cdc_title", "cdc_genres").withColumn("status",lit("A"))

    //Create old inactive records for updated records
    val updatedRecordsUpdated = updatedRecords.select("mst_movieid","mst_title","mst_genres").withColumn("status",lit("I"))

    //Records which are in master but not in cdc file are not changed
    val noChangeRecords = joinResults.where("mst_movieid is not null and cdc_movieid is null").select("mst_movieid","mst_title","mst_genres","mst_status")

    //Create final data set
    val finalDataSet = newRecords.union(updatedRecordsNew).union(updatedRecordsUpdated).union(noChangeRecords)



    finalDataSet.coalesce(1).write.format("com.databricks.spark.csv").save("target/cdc/")

    Thread.sleep(1000*60*60);











  }

}
