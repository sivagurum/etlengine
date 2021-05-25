package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.utils.implicits._
import com.aero.core.EtlFunc._

object MinusDF extends  GenericComponent[Any]{

  
  def execute(resultMap:Map[String,DataFrame], in:Any, out:Any, option:Any, otherOpt:Any):Map[String,DataFrame] ={
            logComponentName("MINUS")
            val df = minusDataFrame(resultMap(in("1")), resultMap(in("2")))
            //other options
            logOutputName(out.toString)
            OTHER_OPTIONS(df, otherOpt, out.toString())
            Map(out.toString() -> df)
          }
    def minusDataFrame(inputDF1: DataFrame, inputDF2: DataFrame): DataFrame = {
    inputDF1.except(inputDF2)
  }
}