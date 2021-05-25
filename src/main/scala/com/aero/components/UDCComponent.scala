package com.aero.components

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

trait UDCComponent extends GenericComponent[Any] {
  
  def init(inputDF: Map[String, DataFrame], outMap:List[String], spark:SparkSession): scala.collection.immutable.Map[String, DataFrame]
  
}