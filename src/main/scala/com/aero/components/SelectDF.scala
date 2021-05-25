package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.utils.implicits._
import com.aero.core.EtlFunc._

object SelectDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    
            logComponentName("SELECT")
            val df = selectDataFrame(resultMap(in.toString), option("select").asInstanceOf[List[String]])

            //otherOptions
            logOutputName(out.toString)
            OTHER_OPTIONS(df, otherOpt, out)
            Map (out.toString() -> df)
          
  }
    def selectDataFrame(inputDF: DataFrame, selectCols: List[String]): DataFrame = {
    inputDF.selectExpr(selectCols: _*)
  }
}