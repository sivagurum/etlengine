package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.utils.implicits._
import com.aero.core.EtlFunc._

object IntersectDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    logComponentName("INTERSECT")
    val df = intersectDataFrame(resultMap(in("1")), resultMap(in("2")))
    //other options
    logOutputName(out.toString)
    OTHER_OPTIONS(df, otherOpt, out.toString())
    Map(out.toString() -> df)

  }
  def intersectDataFrame(inputDF1: DataFrame, inputDF2: DataFrame): DataFrame = {
    inputDF1.intersect(inputDF2)
  }
}