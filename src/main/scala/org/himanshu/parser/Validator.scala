package scala.org.himanshu.parser

import org.apache.commons.lang3.StringUtils
import org.himanshu.helper.ObjectSchema

import scala.org.himanshu.validations.ValidationFunctions

/**
  * Created by himanshu on 8/20/2017.
  */
object Validator {

  def performValidations(row: String, objectSchema: ObjectSchema): (String, String) = {

    val rowArray = row.split(objectSchema.getFileDelimiter);

    var errorCode = "";

    val schema = objectSchema.getSchema;


    if (ValidationFunctions.isRowSizeValid(schema.size(), rowArray.size).isEmpty) {

      var count = 0;
      for (column <- rowArray) {

        if (count < schema.size()) {

          val columnProperties = schema.get(count)

          val dataType = columnProperties.getDataType;
          val validationRequired = columnProperties.getValidationRequired;

          if (validationRequired) {
            if (dataType.equalsIgnoreCase("String")) {

              val result = ValidationFunctions.isValidString(column, columnProperties.getMaxLength)

              if (StringUtils.isNotEmpty(result)) {
                if (errorCode.isEmpty) {
                  errorCode = result + "~" + columnProperties.getColumnName
                } else {
                  errorCode = errorCode + "~" + columnProperties.getColumnName + "|" + result
                }
              }
            }
            if (dataType.equalsIgnoreCase("Integer")) {
              val result = ValidationFunctions.isValidInteger(column)

              if (StringUtils.isNotEmpty(result)) {
                if (errorCode.isEmpty) {
                  errorCode = result + "~" + columnProperties.getColumnName
                } else {
                  errorCode = errorCode + "~" + columnProperties.getColumnName + "|" + result
                }
              }
            }
            if (dataType.equalsIgnoreCase("Date")) {
              val result = ValidationFunctions.isValidDate(column, "YYYYMMDD")

              if (StringUtils.isNotEmpty(result)) {
                if (errorCode.isEmpty) {
                  errorCode = result + "~" + columnProperties.getColumnName
                } else {
                  errorCode = errorCode + "~" + columnProperties.getColumnName + "|" + result
                }
              }
            }


          }
          count = count + 1
        }
      }

    } else {
      errorCode = ValidationFunctions.isRowSizeValid(schema.size(), rowArray.size) + "~" + "size"
    }
    return (errorCode, row)
  }

}
