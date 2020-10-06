/*
* Purpose : Does the profiling like max, min, percentage fill, number of unknown, number of NA, etc of the given dataframe.
* Created By :
* Create Date : 9/18/2018
* Version details: Initial Version
* How to use :
*   import com.ms.psdi.core.dataprofile
    val profileData = DqDataFrameProfile(dataFrame)
    profileData.toDataFrame.show(dataFrame.rdd.count().toInt, false)
*/
package com.ms.psdi.core.daaprofile

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.ms.psdi.core.dataquality.DqColumnProfile

//Entry Class for profiling
case class DqDataFrameProfile(df: DataFrame) {
  lazy val spark = SparkSession.builder().getOrCreate()

  //Profiling the data
  val DqColumnProfiles: List[DqColumnProfile] =
    for (c <- df.columns.toList) yield DqColumnProfile.DqColumnProfileFactory(df, c)

  //Defining the header of the result
  val header: List[String] = List("Column Name", "Column DataType", "Record Count", "Unique Values", "Max Value", "Min Value", "Empty Strings", "Null Values", "Unknown Values", "N/A Values", "Tab Rows","NewLine Rows","CrLf Rows","Percent Fill", "Percent Numeric")
  //"Tab Rows","New Line Rows",

  def toDataFrame: DataFrame = {
    def dfFromListWithHeader(data: List[List[String]], header: String): DataFrame = {
      val rows = data.map { x => Row(x: _*) }
      val rdd = spark.sparkContext.parallelize(rows)
      val schema = StructType(header.split(",").
        map(fieldName => StructField(fieldName, StringType, true)))
      spark.sqlContext.createDataFrame(rdd, schema)
    }

    val data = DqColumnProfiles.map(_.columnData)
    dfFromListWithHeader(data, header.mkString(","))
  }

  override def toString: String = {
    val colummProfileStrings: List[String] = DqColumnProfiles.map(_.toString)
    (header.mkString(",") :: DqColumnProfiles).mkString("\n")
  }
}