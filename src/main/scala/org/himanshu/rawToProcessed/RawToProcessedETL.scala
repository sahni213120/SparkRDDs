package scala.org.himanshu.rawToProcessed

import org.apache.spark.sql._
import org.codehaus.plexus.util.FileUtils
import org.himanshu.helper.{CommandLineArgs, ObjectSchemaCreator}

import scala.org.himanshu.parser.Validator

/**
  * Created by himanshu on 8/5/2017.
  */
object RawToProcessedETL {

  def main(args: Array[String]): Unit = {

    //Verify command line arguments
    val commandLineArgs = new CommandLineArgs(args)

    //Get Spark Session
    val spark = SparkSession.builder().appName("RawToProcessedETL").master("local").getOrCreate()

    //Read source file
    val sourceData = spark.sparkContext.textFile(commandLineArgs.getSourceLocation)

    //Read Source JSON File
    val objectSchema = ObjectSchemaCreator.getObjectSchema(commandLineArgs.getMetaFileLocation)

    //Perform Validations and return error codes
    val newRDD = sourceData.map(x => Validator.performValidations(x, ",", objectSchema))

    //Print new RDD on console
    newRDD.collect().foreach(println)

    //Valid rows are the ones where error code is blank or Null
    val validRDD = newRDD.filter(_._1.isEmpty).values
    validRDD.collect().foreach(println)

    //Reject rows are where error code is not null
    val rejectRDD = newRDD.filter(!_._1.isEmpty).map(x => x._1 + "," + x._2)
    rejectRDD.collect().foreach(println)


    //Write Valid and Reject Output
    FileUtils.deleteDirectory(commandLineArgs.getValidLocation)
    validRDD.saveAsTextFile(commandLineArgs.getValidLocation)

    FileUtils.deleteDirectory(commandLineArgs.getErrorLocation)
    rejectRDD.saveAsTextFile(commandLineArgs.getErrorLocation)


  }


}
