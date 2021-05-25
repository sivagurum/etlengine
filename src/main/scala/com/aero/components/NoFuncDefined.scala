package com.aero.components
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object NoFuncDefined extends  GenericComponent[Any] {
  
  def execute(resultMap:Map[String,DataFrame], in:Any, out:Any, option:Any, otherOpt:Any):Map[String,DataFrame] ={
    
    logError("func option not defined in JSON")
    SparkSession.builder().getOrCreate().stop()
    System.exit(-1)
    resultMap
    
  }
}