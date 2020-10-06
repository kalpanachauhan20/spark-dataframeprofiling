/*
* Purpose : Splits the dataframe into columns and perform operations using functions defined in Functions object.
* Created By :
* Create Date : 9/18/2018
* Version details: Initial Version
*/
package com.ms.psdi.core.dataquality

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.ms.psdi.core.dataquality.DqFunctions._


case class DqColumnProfile(columnName: String
                           , dataType: String
                           , totalDataSetSize: Long
                           , uniqueValues: Long
                           , maxVal: String
                           , minVal: String
                           , emptyStringValues: Long
                           , nullValues: Long
                           , unknownValues: Long
                           , naValues: Long
                           , containTab:Long
                           , containNewLine:Long
                           , containCrLf:Long
                           , numericValues: Long
                          ) {

  lazy val percentFill: Double = calculatedPercentFill(nullValues, emptyStringValues, unknownValues, naValues, totalDataSetSize)
  lazy val percentNumeric: Double = calculatePercentNumeric(numericValues, totalDataSetSize)

  def columnData: List[String] = {
    List(
      columnName
      , dataType
      , totalDataSetSize
      , uniqueValues
      , maxVal
      , minVal
      , emptyStringValues
      , nullValues
      , unknownValues
      , naValues
     ,  containTab
     ,  containNewLine
      ,containCrLf
      , percentFill
      , percentNumeric
    ).map(_.toString)
  }

  def calculatedPercentFill(nullValues: Long, emptyStringValues: Long, unknownValues: Long, naValues: Long, totalRecords: Long): Double = {
    val filledRecords = totalRecords - nullValues - emptyStringValues - unknownValues - naValues;
    percentage(filledRecords, totalRecords)
  }

  def calculatePercentNumeric(numericValues: Long, totalRecords: Long): Double = {
    percentage(numericValues, totalRecords)
  }

  override def toString: String = {
    List(
      columnName
      , dataType
      , totalDataSetSize
      , uniqueValues
      , emptyStringValues
      , nullValues
      , unknownValues
      , naValues
     ,  containTab
     ,  containNewLine
      ,containCrLf
      , percentFill
      , percentNumeric
    ).mkString(",")
  }

}

object DqColumnProfile {

  def DqColumnProfileFactory(df: DataFrame, columnName: String): DqColumnProfile = {
    val dfColumn = df.select(columnName)
    dfColumn.cache
    val recordCount = dfColumn.count()
    val dataType = dfColumn.dtypes(0)._2
    val uniqueValues = dfColumn.distinct().count()
    val emptyCount = dfColumn.withColumn("isEmpty", udfIsEmpty(col(columnName))).filter(col("isEmpty") === true).count
    val nullCount = dfColumn.withColumn("isNULL", udfIsNULL(col(columnName))).filter(col("isNULL") === true).count
    val unknownCount = dfColumn.withColumn("isUnknown", udfIsUnknown(col(columnName))).filter(col("isUnknown") === true).count
    val naCount = dfColumn.withColumn("isNA", udfIsNA(col(columnName))).filter(col("isNA") === true).count
    //Added tab
    val tabCount = dfColumn.withColumn("hasTab", udfHasTab(col(columnName))).filter(col("hasTab") === true).count
    val newLineCount = dfColumn.withColumn("hasNewLine", udfHasNewLine(col(columnName))).filter(col("hasNewLine") === true).count
    val crLfCount = dfColumn.withColumn("hasCrLf", udfHasCrLf(col(columnName))).filter(col("hasCrLf") === true).count
    val numericCount = dfColumn.withColumn("isNumeric", udfIsNumeric(col(columnName))).filter(col("isNumeric") === true).count
    var maxVal = ""
    var minVal = ""
    if (dataType == "TimestampType" || dataType == "DateType" || dataType == "IntegerType" || dataType == "ShortType"|| dataType == "LongType"|| dataType == "ByteType" || dataType == "FloatType" || dataType == "DoubleType"|| dataType == "DecimalType") {
      val agg = dfColumn.agg(min(columnName), max(columnName)).head()
      if(agg.get(0) != null)
      minVal = agg.get(0).toString()
      if(agg.get(1) != null)
      maxVal = agg.get(1).toString()
    }

    new DqColumnProfile(columnName, dataType, recordCount, uniqueValues, maxVal.toString(), minVal.toString(), emptyCount, nullCount, unknownCount,naCount,tabCount,newLineCount, crLfCount,numericCount)
  }

}
