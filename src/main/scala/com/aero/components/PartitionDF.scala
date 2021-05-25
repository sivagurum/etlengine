package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.sql.functions.expr

object PartitionDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {

    logComponentName("PARTITION")
    val partitionConfig = option.asInstanceOf[Map[String, Any]]
    val numOfPartitions =
      if (partitionConfig.getOrElse("numberOfPartitions", 0) == 0)
        0
      else {
        if (partitionConfig("numberOfPartitions").isInstanceOf[String])
          partitionConfig("numberOfPartitions").asInstanceOf[String].toInt
        else
          partitionConfig("numberOfPartitions").asInstanceOf[BigInt].toInt
      }

    val df = PARTITION(
      resultMap(in.toString),
      partitionConfig("partitionByColumns").asInstanceOf[List[String]],
      numOfPartitions)
    logOutputName(out.toString)
    OTHER_OPTIONS(df, otherOpt, out.toString())
    Map(out.toString() -> df)

  }
  def partitionDataFrame(inputDF: DataFrame, partitionCols: List[String], numOfPartitions: Int): DataFrame = {
    if (numOfPartitions == 0) {
      inputDF.repartition(partitionCols.map(expr(_)): _*)
    } else {
      inputDF.repartition(numOfPartitions, partitionCols.map(expr(_)): _*)
    }
  }
}