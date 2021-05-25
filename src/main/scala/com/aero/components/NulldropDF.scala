package com.aero.components

import org.apache.spark.sql.DataFrame

object NulldropDF {
    def nullDropDataFrame(inputDF: DataFrame, nullDropOptions: (String, List[String])): DataFrame = {
    nullDropOptions match {
      case (anyORall, nullDropCols) if !anyORall.isEmpty & !nullDropCols.isEmpty => inputDF.na.drop(anyORall, nullDropCols)
      case (anyORall, nullDropCols) if anyORall.isEmpty & !nullDropCols.isEmpty => inputDF.na.drop(nullDropCols)
      case (anyORall, nullDropCols) if !anyORall.isEmpty & nullDropCols.isEmpty => inputDF.na.drop(anyORall)
      case _ => inputDF
    }
  }
}