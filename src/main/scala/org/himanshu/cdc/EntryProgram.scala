package scala.org.himanshu.cdc

import org.apache.spark.sql.SparkSession
import org.codehaus.plexus.util.FileUtils
import org.himanshu.helper.{CDCProperties, CommandLineArgs}

import scala.org.himanshu.cdc.helper.ReadWriteService

/**
  * Generic Entry point for spark for common CDC operations
  * Created by himanshu on 6/4/2017.
  */
object EntryProgram {

  def main(args: Array[String]): Unit = {
    //Verify command line arguments
    val commandLineArgs = new CommandLineArgs(args)

    //Convert input json to an object
    val cdcProperties = CDCProperties.getCDCProperties(commandLineArgs.getMetaFileLocation)

    //Create spark session
    val spark = SparkSession.builder().appName(cdcProperties.getJobName).master("local").getOrCreate()

    //Read master data
    val master = new ReadWriteService(spark, commandLineArgs.getMasterLocation, cdcProperties.getTargetHeaderString,
      cdcProperties.getFileDelimiter).readAndGetDF()

    //Read changed data
    val cdc = new ReadWriteService(spark, commandLineArgs.getSourceLocation, cdcProperties.getSourceHeaderString,
      cdcProperties.getFileDelimiter).readAndGetDF()

    //Perform Transformations
    val finalOutput = new PerformCDC(cdc, master, cdcProperties).run

    //Write final output to disk

    finalOutput.write.format("com.databricks.spark.csv").save(commandLineArgs.getValidLocation)

  }

}
