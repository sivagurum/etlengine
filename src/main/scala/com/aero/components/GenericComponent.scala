package com.aero.components
import org.apache.spark.sql.DataFrame
import com.aero.logging.Logging

trait GenericComponent[T>:Null<:Any] extends Logging {
  
  def execute(resultMap:Map[String,DataFrame], in:T, out:T, option:T, otherOpt:T):Map[String,DataFrame]
  
}