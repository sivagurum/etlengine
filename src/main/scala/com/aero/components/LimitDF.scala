package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._


object LimitDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    
            logComponentName("LIMIT")
            val limitConfig = option.asInstanceOf[Map[String, String]]
            val df = limitDataFrame(resultMap(in.toString), option("limit"))
            logOutputName(out.toString)
            OTHER_OPTIONS(df, otherOpt, out.toString())
            Map(out.toString() -> df)
          
  }
    def limitDataFrame(inputDF: DataFrame, limitOption: String): DataFrame = {
    if (limitOption != "N" && limitOption != "") inputDF.limit(limitOption.toInt) else inputDF
  }
}