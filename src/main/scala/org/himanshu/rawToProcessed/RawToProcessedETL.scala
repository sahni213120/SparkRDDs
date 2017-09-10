package scala.org.himanshu.rawToProcessed

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.himanshu.helper.{CommandLineArgs, ObjectSchema}

import scala.org.himanshu.parser.Validator
import scala.org.himanshu.utils.ReadWriteService

/**
  * Created by himanshu on 8/27/2017.
  */
class RawToProcessedETL(spark: SparkSession, objectSchema: ObjectSchema) extends Serializable {

  def extract(location: String, format: String): RDD[String] = {
    ReadWriteService.read(spark, location, format)
  }

  def tranform(sourceRDD: RDD[String]): RDD[(String, String)] = {

    //Perform Validations and return error codes
    val newRDD = sourceRDD.map(x => Validator.performValidations(x, objectSchema))
    newRDD.persist()
    return newRDD

  }

  def getValidRDD(rdd: RDD[(String, String)]): RDD[String] = {

    //Valid rows are the ones where error code is blank or Null
    val validRDD = rdd.filter(_._1.isEmpty).values

    return validRDD


  }

  def getRejectRDD(rdd: RDD[(String, String)]): RDD[String] = {
    //Reject rows are where error code is not null
    val rejectRDD = rdd.filter(!_._1.isEmpty).map(x => x._1 + "," + x._2)
    rejectRDD.persist()

    return rejectRDD

  }


  def getRejectionSummaryRDD(rdd: RDD[(String, String)]): RDD[String] = {
    val summaryRejectionsRDD = rdd.filter(!_._1.isEmpty).keys.map(x => (x, 1)).reduceByKey((x, y) => (x + y))
      .map(x => {
        val y = x._1.split("~")
        y(0) + "," + y(1) + "," + x._2
      })

    return summaryRejectionsRDD

  }

  def validate(sourceCount: Long, validCount: Long, rejectCount: Long): Unit = {
    if (sourceCount != validCount + rejectCount) {
      throw new RuntimeException("Counts are not matching")
    }
  }

  def load(validRDD: RDD[String], validLocation: String, rejectRDD: RDD[String], rejectLocation: String, rejectSummaryRDD: RDD[String], rejectSummaryLocation: String): Unit = {
    ReadWriteService.write(validRDD, validLocation, "Text")
    ReadWriteService.write(rejectRDD, rejectLocation, "Text")
 //   ReadWriteService.write(rejectSummaryRDD, rejectSummaryLocation, "Text")
  }

  def run(commandLineArgs: CommandLineArgs): Unit = {
    //Extract Source Data
    val sourceData = extract(commandLineArgs.getSourceLocation, "Text")

    //Perform Transformations and Validations
    val newRDD = tranform(sourceData)

    //Get Valid Data
    val validRDD = getValidRDD(newRDD)
    //

    //Get Rejected Data
    val rejectRDD = getRejectRDD(newRDD)

    //Get Rejection Summary
    val summaryRejectionsRDD = getRejectionSummaryRDD(newRDD)

    validate(sourceData.count(), validRDD.count(), rejectRDD.count())

    load(validRDD, commandLineArgs.getValidLocation, rejectRDD,
      commandLineArgs.getErrorLocation, summaryRejectionsRDD, "target/rejectSum")

  }

}
