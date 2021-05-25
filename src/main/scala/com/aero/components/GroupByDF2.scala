package com.aero.components

import com.aero.core.EtlFunc._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{expr, col}
import com.aero.core.aeroDriver._
import com.aero.utils.implicits._

object GroupByDF2 extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    
            logComponentName("GROUPBY")
            val groupbyConfig = option.asInstanceOf[Map[String, Any]]
            //optional fields
            val groupKey =
              if (groupbyConfig.getOrElse("groupbyKeys", List.empty[String]) != null)
                groupbyConfig.getOrElse("groupbyKeys", List.empty[String]).asInstanceOf[List[String]].map(resolveParamteres(_))
              else List(null)
            //call groupby
            val df = groupByDataFrameE(
              resultMap(in.toString),
              groupKey,
              groupbyConfig("aggregation").asInstanceOf[List[List[String]]].map(x => (resolveParamteres(x(0)), resolveParamteres(x(1)), resolveParamteres(x(2)))))
            
              //Storage level
            val storageLevel = resolveParamteres(option.getOrElse("storageLevel", "N"))
            if (storageLevel != "N") STORAGE(Map(df -> storageLevel))
            //otherOptions
            logOutputName(out.toString)
            OTHER_OPTIONS(df, otherOpt, out.toString())
            Map (out.toString() -> df)
          
  }
  
    def groupByDataFrameE(inputDF: DataFrame, gCols: List[String], aggCond: List[(String, String, String)]): DataFrame = {

    //validate all inputs are in same pattern
    def aggCondFinal(aggPatternList: List[(String, String, String)]): List[(Int, String, String, String)] = {
      aggPatternList match {
        case (gFunc, "*", "*") :: aggTail => {
          val aggPatt = for (aggCols <- inputDF.columns.toList diff (gCols)) yield (1, gFunc, aggCols, gFunc + "_" + aggCols)
          aggPatt ++ aggCondFinal(aggTail)
        }
        case (gFunc, "*", aliasPattern) :: aggTail => {
          val patternSuffixAlias = "\\* \\w+".r
          val suffixAlias = patternSuffixAlias.findFirstIn(aliasPattern).getOrElse("").toString()
          val patternPrefixAlias = "\\w+ \\*".r
          val prefixAlias = patternPrefixAlias.findFirstIn(aliasPattern).getOrElse("").toString()

          if (aliasPattern == prefixAlias) {
            val prefixCol = prefixAlias.split(" ")(0)
            val aggPatt = for (aggCols <- inputDF.columns.toList diff (gCols)) yield (2, gFunc, aggCols, prefixCol + aggCols)
            aggPatt ++ aggCondFinal(aggTail)
          } else {
            val suffixCol = suffixAlias.split(" ")(1)
            for (aggCols <- inputDF.columns.toList diff (gCols)) yield (2, gFunc, aggCols, aggCols + suffixCol)
            aggCondFinal(aggTail)
          }
        }
        case aggHead :: aggTail => {
          List((3, aggHead._1, aggHead._2, aggHead._3)) ++ aggCondFinal(aggTail)
        }
        case Nil => {
          List()
        }
      }
    }
    val aggCondFinalList = aggCondFinal(aggCond)
    //aggCondFinalList.foreach(println)
    val isAggSamePattern = if (aggCondFinalList.map(x => x._1).distinct.length > 1) false else true
    if (!isAggSamePattern) {
      EXIT(inputDF, Map("errorMessage" -> s"aggregation has different patterns: '${aggCond}'", "exitRC" -> "-1"))
    }
    val gropyByFuncFirst = { x: (String, String, String) => x._1 + '(' + x._2 + ')' + " as " + x._3 }
    val grpFirstStatement = (aggCondFinalList.head._2, aggCondFinalList.head._3, aggCondFinalList.head._4)
    val gropyByFuncRest = for (x <- aggCondFinalList.tail) yield expr(gropyByFuncFirst(x._2, x._3, x._4))

    if (gCols.isEmpty || gCols == List(null))
      inputDF.agg(expr(gropyByFuncFirst(grpFirstStatement)), gropyByFuncRest: _*)
    else
      inputDF.groupBy(gCols.map(c => col(c)): _*).agg(expr(gropyByFuncFirst(grpFirstStatement)), gropyByFuncRest: _*)
  }
  
}