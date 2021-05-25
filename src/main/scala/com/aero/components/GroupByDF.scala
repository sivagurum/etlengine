package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.sql.functions.{expr, col}


object GroupByDF {
    def groupByDataFrame(inputDF: DataFrame, gCols: List[String], aggCond: List[(String, String, String)]): DataFrame = {
    val aggCondFinal =
      aggCond.head match {
        case (gFunc, "*", "*") => {
          for (aggCols <- inputDF.columns.toList diff (gCols)) yield (gFunc, aggCols, aggCols)
        }
        case (gFunc, "*", aliasPattern) => {
          val patternSuffixAlias = "\\* \\w+".r
          val suffixAlias = patternSuffixAlias.findFirstIn(aliasPattern).getOrElse("").toString()
          val patternPrefixAlias = "\\w+ \\*".r
          val prefixAlias = patternPrefixAlias.findFirstIn(aliasPattern).getOrElse("").toString()

          if (aliasPattern == prefixAlias) {
            val prefixCol = prefixAlias.split(" ")(0)
            for (aggCols <- inputDF.columns.toList diff (gCols)) yield (gFunc, aggCols, prefixCol + aggCols)
          } else {
            val suffixCol = suffixAlias.split(" ")(1)
            for (aggCols <- inputDF.columns.toList diff (gCols)) yield (gFunc, aggCols, aggCols + suffixCol)
          }
        }
        case _ => aggCond
      }
    val gropyByFuncFirst = { x: (String, String, String) => x._1 + '(' + x._2 + ')' + " as " + x._3 }
    val gropyByFuncRest = for (x <- aggCondFinal.tail) yield expr(gropyByFuncFirst(x))

    inputDF.groupBy(gCols.map(c => col(c)): _*).agg(expr(gropyByFuncFirst(aggCondFinal.head)), gropyByFuncRest: _*)
  }
}