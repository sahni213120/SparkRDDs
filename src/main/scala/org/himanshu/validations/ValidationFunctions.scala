package scala.org.himanshu.validations

import java.text.SimpleDateFormat

import org.apache.commons.lang.StringUtils

/**
  * Created by himanshu on 8/6/2017.
  *
  */
object ValidationFunctions {

  //TODO : Use enums instead of hard coded values
  /**
    * Valid a row size with expected and actual
    * @param expectedSize
    * @param actualSize
    * @return error code is invalid else blank
    */
  def isRowSizeValid(expectedSize: Integer, actualSize: Integer): String = {
    if (expectedSize != actualSize) {
      return "1001"
    } else {
      return ""
    }

  }


  //TODO : Use enums instead of hard coded values
  def isValidString(value: String, maxLength: Integer): String = {

    if (StringUtils.isNotBlank(value) && value.length > maxLength)
      return "2001"
    else
      return ""
  }

  //TODO : Use enums instead of hard coded values
  def isValidInteger(value: String): String = {

    if (StringUtils.isNumeric(value))
      return ""
    else
      return "2002"
  }

  //TODO : Use enums instead of hard coded values
  def isValidDate(value: String, dateFormat: String): String = {

    val format = new SimpleDateFormat(dateFormat)
    format.setLenient(false)

    try {
      format.parse(value)
      return ""
    } catch {
      case ex: java.text.ParseException => return "2003"
    }
  }

  def main(args: Array[String]): Unit = {
    println(isValidDate("20160101", "YYYYMMDD"))
  }


}
