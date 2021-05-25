package com.aero.components

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._


object SQLDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    
            logComponentName("SQL")

            val sqlConfig = option.asInstanceOf[Map[String, String]]

            val df = SQL(
              SparkSession.builder().getOrCreate(),
              in.map(entry => (entry._2 -> resultMap(entry._2))),
              option("sql"))

            logOutputName(out.toString)
            OTHER_OPTIONS(df, otherOpt, out.toString)

            Map (out.toString() -> df)
          
  }
    def sqlDataFrameE(ss: SparkSession, inputDFs: Map[String, DataFrame], sqlStatement: String): DataFrame = {
    inputDFs.map { case (k, v) => v.createOrReplaceTempView(k) }
    ss.sql(sqlStatement)
  }
}