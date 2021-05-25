package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.utils.implicits._

object SortPartitionDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    
            logComponentName("SORT_PARTITION")
            val df =
              sortPartitionDataFrame(
                resultMap(in.toString),
                option("sortKeys").asInstanceOf[List[String]])

            logOutputName(out.toString)
            OTHER_OPTIONS(df, otherOpt, out)
            
            Map (out.toString() -> df)
          
  }
  def sortPartitionDataFrame(inputDF: DataFrame, partitionBy: List[String]): DataFrame = {
    if (partitionBy.isEmpty) inputDF.sortWithinPartitions()
    else inputDF.sortWithinPartitions(partitionBy.head, partitionBy.tail: _*)
  }
}