package scala.org.himanshu.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.codehaus.plexus.util.FileUtils


/**
  * Created by himanshu on 8/27/2017.
  */
object ReadWriteService {

  def read(spark: SparkSession, location: String, format: String): RDD[String] = {

    format match {
      case "Text" => return spark.sparkContext.textFile(location)
    }

  }

  def write(rdd: RDD[String], location: String, format: String): Unit = {
    FileUtils.deleteDirectory(location)

    format match {
      case "Text" => rdd.saveAsTextFile(location)
    }
  }

  def write(df: DataFrame, location: String, format: String): Unit = {
    FileUtils.deleteDirectory(location)

    format match {
      case "Parquet" => df.write.parquet(location)
    }
  }


}
