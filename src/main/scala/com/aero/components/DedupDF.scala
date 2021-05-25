package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.utils.implicits._

object DedupDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {

    logComponentName("DEDUP")
    val dedupConfig = option.asInstanceOf[Map[String, Any]]
    val df = dedupDataFrame(
      resultMap(in.toString),
      dedupConfig("keys").asInstanceOf[List[String]],
      (dedupConfig -- List("keys")))
    val mout = Map(out("unique") -> df._1, out.getOrElse("duplicate", "_") -> df._2)
    val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]
    if (!otherOptKV.isEmpty) {
      for ((k, v) <- otherOptKV) {
        logOutputName(k.toString)
        OTHER_OPTIONS(mout(k), v, k)
      }
    }
    mout
  }

  def dedupDataFrame(inputDF: DataFrame, dupCols: List[String], deDupConfig: Map[String, String]): (DataFrame, DataFrame) = {
    val dupOption = deDupConfig.getOrElse("keep", "first")
    val removeDuplicatesDF = DROP_DUPLICATES(inputDF, dupCols, dupOption)
    if (deDupConfig.getOrElse("captureDuplicates", "N") == "Y")
      (removeDuplicatesDF, inputDF.except(removeDuplicatesDF))
    else
      (removeDuplicatesDF, removeDuplicatesDF.limit(0))
  }

}