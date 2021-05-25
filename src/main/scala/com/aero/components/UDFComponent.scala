package com.aero.components

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import com.aero.logging.Logging

trait UDFComponent extends Logging {
  
  def init(spark:SparkSession):UserDefinedFunction
  
}