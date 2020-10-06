/*
* Purpose : Functions to help the DataColumnProfile class to perform operations on columns.
* Created By :
* Create Date : 9/18/2018
* Version details: Initial Version
*/
package com.ms.psdi.core.dataquality

import java.text.DecimalFormat
import scala.util.Try
import org.apache.spark.sql.functions._


object DqFunctions {

  val udfIsNULL = udf[Boolean, String](isNULL)
  val udfIsNA = udf[Boolean, String](isNA)
  val udfIsUnknown = udf[Boolean, String](isUnknown)
  val udfIsEmpty = udf[Boolean, String](isEmpty)
  val udfIsNumeric = udf[Boolean, String](isNumeric)
  val udfHasTab= udf[Boolean, String](hasTab)
  val udfHasNewLine = udf[Boolean, String](hasNewLine)
  val udfHasCrLf = udf[Boolean, String](hasCrLf)


  def percentage(numerator: Double, denominator: Double): Double = {
    round((numerator.toDouble / denominator.toDouble) * 100)
  }

  def round(x: Double): Double = {
    val formatString = "#####.#"
    val formatter = new DecimalFormat(formatString)
    val result = formatter.format(x)
    result.toDouble
  }

  def isEmpty(x: String): Boolean = {
    if (Option(x) == None) return false
    x.trim == ""
  }

  def isNULL(x: String): Boolean = {
    if (x == "#NULL#") {
      return true
    }
    else if(x == "null") {
      return true
    }
    else if (x == null)
      return true
    else return false
  }

  def isUnknown(x: String): Boolean = {
    if (x == "UNKNOWN") {
      return true
    }
    else return false
  }

  def isNA(x: String): Boolean = {
    if (x == "N/A") {
      return true
    } // null values are not empty by definition
    else return false
  }

  def isNumeric(x: String): Boolean = {
    val z: Option[Float] = Try(x.toFloat).toOption
    z != None
  }
//Check whether data has tab in it
  def hasTab(x: String): Boolean = {
    if (x!=null && x.contains("\t") ==true) {
     return true
    }
    else return false
  }

  //Check whether data has new line in it
  def hasNewLine(x: String): Boolean = {
   if (x!=null && x.contains("\n") ==true) {
    return true
   }
   else return false
  }

  //Check whether data has carriage return and newline using ascii value
  def hasCrLf(x: String): Boolean = {
    if (x!=null && (x.contains(13.toChar) ==true && x.contains(10.toChar) ==true )) {
      return true
    }
    else return false
  }


}