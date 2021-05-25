package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._

object MultiFilterDF {
  
    def multiFilterDataFrame(inputDF: DataFrame, filterConfig: Map[String, String]): Map[String, DataFrame] = {
    for ((condSeq, filterCondition) <- filterConfig) yield {
      val filteredDF =
        filterCondition match {
          case "head" => inputDF.limit(1)
          case "tail" => {
            val getLastCond = (for (colsDF <- inputDF.columns.toList) yield ("last", colsDF, colsDF))
            GROUPBY(inputDF, Nil, getLastCond)
          }
          case cond if cond.contains("skip") => {
            val skipPattern = "skip\\((\\d+)\\)".r
            val skipPattern(skipCount) = cond
            inputDF.except(inputDF.limit(skipCount.toInt).selectExpr(inputDF.columns.toList.map(c => c + " as " + c): _*))
          }
          case cond if cond.contains("get") => {
            val skipPattern = "get\\((\\d+)\\)".r
            val skipPattern(getCount) = cond
            FILTER(inputDF.limit(getCount.toInt), Map("filterCond" -> "tail"))._1
          }
          case _ => inputDF.filter(filterConfig(condSeq))
        }
      (condSeq, filteredDF)
    }
  }
  
}