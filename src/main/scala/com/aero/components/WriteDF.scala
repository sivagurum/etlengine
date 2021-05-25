package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
import com.aero.core.aeroDriver._

object WriteDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {

    logComponentName("WRITE")
    val outputFileConfig =
      for ((k, v) <- out.asInstanceOf[Map[String, String]]) yield {
        (k, resolveParamteres(v))
      }
    //output
    writeDataFrame(resultMap(in.toString), outputFileConfig)
    resultMap

  }

  def writeDataFrame(inputDF: DataFrame, writeConfig: Map[String, String]):Int = {
    writeConfig("fileFormat") match {
      case "parquet" => { inputDF.write.parquet(writeConfig("filePath")); 0 }
      case "json" => { inputDF.write.json(writeConfig("filePath")); 0 }
      case "csv" => { inputDF.write.options(writeConfig).csv(writeConfig("filePath")); 0 }
      case fileType => {
        inputDF
          .write.format(fileType)
          .options(writeConfig -- List("fileFormat", "filePath"))
          .save(writeConfig("filePath"));0
      }
    }
  }
}