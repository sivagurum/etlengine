package com.aero.components

import org.apache.spark.sql.DataFrame

object ColumnFunctionsDF {
  
    def columnFunctionDataFrame(inputDF: DataFrame, columnsFuncMap: Map[String, String]): String = {
    val cFuncList = columnsFuncMap.toList(0)
    inputDF.selectExpr(s"${cFuncList._2}(${cFuncList._1})").collect.mkString.replace("[", "").replace("]", "")
  }
}