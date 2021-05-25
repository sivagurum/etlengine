package com.aero.custom.udf

import com.aero.components.UDFComponent
import org.apache.spark.sql.SparkSession


object LengthUDF extends UDFComponent {
  
  override def init(spark:SparkSession)={
    logInfo("Registering UDF lengthUDF")
    val lengthUDF = spark.udf.register("lengthUDF", length _)
    lengthUDF
  }
  
  def length(name:Any)={
    name.toString.length
  }
  
}