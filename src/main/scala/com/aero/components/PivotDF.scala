package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.sql.functions.expr

object PivotDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {

    logComponentName("PIVOT")
    val df =
      pivotDataFrame(
        resultMap(in.toString),
        option("groupbyKeys").asInstanceOf[List[String]],
        option("pivotColumn"),
        option("aggregation").asInstanceOf[List[List[String]]].map(x => (x(0), x(1))))
    logOutputName(out.toString)
    OTHER_OPTIONS(df, otherOpt, out.toString())
    Map(out.toString() -> df)

  }
  def pivotDataFrame(
    inputDF: DataFrame,
    keyCol: List[String],
    pivotCol: String,
    aggCondList: List[(String, String)]): DataFrame = {

    val aggCond = aggCondList.map { case (pFunc, tCol) => pFunc + " as " + tCol }
    inputDF.groupBy(keyCol.map(expr(_)): _*).pivot(pivotCol).agg(expr(aggCond.head), aggCond.tail.map(expr(_)): _*)
  }
}