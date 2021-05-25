package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._


object UnionDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    
            logComponentName("UNION")
            val inputKV = in.asInstanceOf[List[String]]
            val df = UNION(inputKV.map(resultMap))

            //other options
            logOutputName(out.toString)
            OTHER_OPTIONS(df, otherOpt, out.toString)
            Map(out.toString() -> df)
          
  }
    def unionDataFrame(inputDFs: DataFrame*): DataFrame = {
    inputDFs.reduce((aDF, bDF) => aDF.union(bDF))
  }
}