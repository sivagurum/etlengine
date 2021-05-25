package com.aero.components

import org.apache.spark.sql.DataFrame

object RenameColumnsDF {
    def renameColumnsDataFrame(inputDF: DataFrame, columnsList: List[String]): DataFrame = {
    inputDF.toDF(columnsList: _*)
  }
}