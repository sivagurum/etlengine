package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.utils.implicits._
import org.apache.spark.sql.functions.broadcast


object LookupDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    
            logComponentName("LOOKUP")

            val df = lookupDataFrameE(
              resultMap(in("input")),
              resultMap(in("lookup")),
              option("keys").asInstanceOf[List[String]],
              option.getOrElse("lookupKeys", Nil).asInstanceOf[List[String]],
              option("lookupColumn"))

            logOutputName(out.toString)
            OTHER_OPTIONS(df, otherOpt, out.toString())

           Map (out.toString() -> df)
          
  }
    def lookupDataFrame(inputDF: DataFrame, lookupDF: DataFrame, lookupCol: String, lookupKey: String*): DataFrame = {
    inputDF.join(broadcast(lookupDF), lookupKey, "left").drop(lookupKey: _*)
  }
    def lookupDataFrameE(inputDF: DataFrame, lookupDF: DataFrame, inputKey: List[String], lookupKey: List[String], lookupCol: String): DataFrame = {
    lookupKey match {
      case lk if lk.isEmpty || inputKey == lk => {
        val lookupJoinCols = inputKey.zip(inputKey).map { case (key1, key2) => inputDF(key1) === lookupDF(key2) }.reduce(_ && _)
        inputDF.join(broadcast(lookupDF), lookupJoinCols, "left").drop(lookupKey: _*)
      }
      case _ => {
        val renamedLookupKey = lookupKey.map(_ + "_lookup")
        val aliasLookupKey = lookupKey.map(col => s"${col} as ${col}_lookup")
        val lookupJoinCols = inputKey.zip(renamedLookupKey).map { case (key1, key2) => inputDF(key1) === lookupDF(key2) }.reduce(_ && _)
        inputDF.join(broadcast(lookupDF.selectExpr(aliasLookupKey :+ lookupCol: _*)), lookupJoinCols, "left").drop(renamedLookupKey: _*)
      }
    }
  }
}