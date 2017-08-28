package scala.org.himanshu.rawToProcessed

import org.apache.spark.sql._
import org.himanshu.helper.{CommandLineArgs, ObjectSchemaCreator}

/**
  * Created by himanshu on 8/5/2017.
  */
object ETLDriver {

  def main(args: Array[String]): Unit = {

    //Verify command line arguments
    val commandLineArgs = new CommandLineArgs(args)

    //Get Spark Session
    val spark = SparkSession.builder().appName("RawToProcessedETL").master("local").getOrCreate()

    //Read Source JSON File and convert it into an object
    val objectSchema = ObjectSchemaCreator.getObjectSchema(commandLineArgs.getMetaFileLocation)

    //Create instance of ETL
    val rawToProcessedETL = new RawToProcessedETL(spark, objectSchema)

    //Run the ETL Process
    rawToProcessedETL.run(commandLineArgs)

  }


}
