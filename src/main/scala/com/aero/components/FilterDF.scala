package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
 
object FilterDF extends Logging with  GenericComponent[Any]{

  
  def execute(resultMap:Map[String,DataFrame], in:Any, out:Any, option:Any, otherOpt:Any):Map[String,DataFrame] ={
            
            logComponentName("FILTER")
            val filterConfig = option.asInstanceOf[Map[String, String]]
            val df = filterDataFrame(resultMap(in.toString), filterConfig)
            //assign filter output. Output can contain multiple outputs
            val mout = List(out("select"),out.getOrElse("deSelect", "_"))
              .zip(df.productIterator.toList).map(x =>(x._1 -> x._2.asInstanceOf[DataFrame])).toMap
            //storage level
            if (option.getOrElse("storageLevel", "N") != "N") {
              val storageConfig = option.getOrElse("storageLevel", Map.empty[String, String]).asInstanceOf[Map[String, String]]
              STORAGE(storageConfig.map(x => (resultMap(x._1), x._2)))
            }
            val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]
            if (!otherOptKV.isEmpty) {
              for ((k, v) <- otherOptKV) {
                logOutputName(k.toString)
                OTHER_OPTIONS(mout(k), v, k)
              }
            }
            mout
  }
  
  def filterDataFrame(inputDF: DataFrame, filterConfig: Map[String, String]): (DataFrame, DataFrame) = {
    val filterCondition = filterConfig("filterCond")
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
          filterDataFrame(inputDF.limit(getCount.toInt), Map("filterCond" -> "tail"))._1
        }
        case _ => inputDF.filter(filterConfig("filterCond"))
      }
    if (filterConfig.getOrElse("deSelect", "N") == "N")
      (filteredDF, filteredDF.limit(0))
    else (filteredDF, inputDF.except(filteredDF))
  }
  
}