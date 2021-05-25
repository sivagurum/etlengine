package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.sql.functions.{ col, expr }
import org.apache.spark.sql.expressions.Window
import com.aero.core.EtlFunc._

object CumulativeDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {

    logComponentName("CUMULATIVE")

    val df = cumulativeDataFrame(
      resultMap(in.toString),
      option("keys").asInstanceOf[List[String]],
      option("orderBy").asInstanceOf[List[String]],
      (option("cumulativeFunction"), option("cumulativeColumn"), option("targetColumn")),
      'N')

    logOutputName(out.toString)
    OTHER_OPTIONS(df, otherOpt, out.toString())
    Map(out.toString() -> df)

  }
  def cumulativeDataFrame(
    inputDF: DataFrame,
    keyCol: List[String],
    orderCol: List[String],
    cumlativeFuncCol: (String, String, String),
    dropSorurceCumCol: Char): DataFrame = {

    val cumPartition = Window.partitionBy(keyCol.map(col(_)): _*).orderBy(orderCol.map(col(_)): _*)
    val cumString = cumlativeFuncCol._1 + '(' + cumlativeFuncCol._2 + ')'
    inputDF.withColumn(
      if (cumlativeFuncCol._3 > " ") cumlativeFuncCol._3 else cumlativeFuncCol._1 + cumlativeFuncCol._2, expr(cumString).over(cumPartition))
      .drop(if (dropSorurceCumCol == 'Y') cumlativeFuncCol._2 else "")
  }

}