package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.utils.implicits._

object DiffDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    logComponentName("DIFF")
    val df = diffDataFrame(resultMap(in("1")), resultMap(in("2")))
    val mout = Map(out("1") -> df._1, out("2") -> df._2, out("3") -> df._3)
    //other options
    val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]
    if (!otherOptKV.isEmpty) {
      for ((k, v) <- otherOptKV) {
        logOutputName(k.toString)
        OTHER_OPTIONS(mout(k), v, k)
      }
    }
    mout
  }
  def diffDataFrame(inputDF1: DataFrame, inputDF2: DataFrame): (DataFrame, DataFrame, DataFrame) = {
    (inputDF1.except(inputDF2), inputDF2.except(inputDF1), inputDF1.intersect(inputDF2))
  }
}