package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.utils.implicits._

object DropDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    
            logComponentName("DROP")
            val df = dropDataFrame(resultMap(in.toString), option("drop").asInstanceOf[List[String]])

            //otherOptions
            logOutputName(out.toString)
            OTHER_OPTIONS(df, otherOpt, out.toString())
            Map (out.toString() -> df)
          
  }
    def dropDataFrame(inputDF: DataFrame, dropCols: List[String]): DataFrame = {
    inputDF.drop(dropCols: _*)
  }
}