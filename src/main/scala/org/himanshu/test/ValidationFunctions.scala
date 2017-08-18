package scala.org.himanshu.test

import java.text.SimpleDateFormat

import org.apache.commons.lang.StringUtils

/**
  * Created by himanshu on 8/6/2017.
  */
object ValidationFunctions {


  def isValidString(value: String, maxLength: Int): String = {

    if (StringUtils.isNotBlank(value) && value.length > maxLength)
      return "123"
    else
      return ""
  }

  def isValidInteger(value: String): Boolean = {

    if (StringUtils.isNumeric(value))
      return true
    else
      return false
  }

  def isValidDate(value: String, dateFormat: String): Boolean = {

    val format = new SimpleDateFormat(dateFormat)
    format.setLenient(false)

    try {
      format.parse(value)
      return true
    } catch {
      case ex : java.text.ParseException => return false
    }
  }

  def main (args : Array[String]): Unit =
  {
    println(isValidDate("20160101", "YYYYMMDD"))
  }


}
