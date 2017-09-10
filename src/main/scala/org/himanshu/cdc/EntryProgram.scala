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

    //Perform CDC
    val finalOutput = new PerformCDC(cdcProperties, spark).run(commandLineArgs.getValidLocation)


  }

}
