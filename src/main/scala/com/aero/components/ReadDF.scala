package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.sql.SparkSession
import com.aero.core.aeroDriver._
import java.util.Properties

object ReadDF extends Logging {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {

    logComponentName("READ")

    val inputFileConfig = in.map(kv => (kv._1 -> resolveParamteres(kv._2)))
    val df = readDataFrame(spark, inputFileConfig)
    logOutputName(out.toString)
    OTHER_OPTIONS(df, otherOpt, out.toString())
    Map(out.toString() -> df)

  }
  
  def readDataFrame(ss: SparkSession, fileConfig: Map[String, String]): DataFrame = {
    fileConfig("fileFormat").toString.toLowerCase() match {
      case x @ "parquet|json|csv|orc|text|textfile" => ss.read.format(x).options(fileConfig -- List("fileFormat", "filePath")).load(fileConfig("filePath")) 
      case "jdbc" if (fileConfig.contains("lowerBound") || fileConfig.contains("upperBound")) => {
        val props = new Properties()
        (fileConfig -- List("url", "table", "columnName", "lowerBound", "upperBound", "numPartitions")).foreach(kv => props.put(kv._1, kv._2))
        spark.read.jdbc(
          fileConfig("url"),
          fileConfig("table"),
          fileConfig("columnName"),
          fileConfig("lowerBound").toLong,
          fileConfig("upperBound").toLong,
          fileConfig("numPartitions").toInt,
          props)
      }
      case "jdbc" if (fileConfig.contains("predicates")) => {
        val props = new Properties()
        (fileConfig -- List("url", "table", "predicates")).foreach(kv => props.put(kv._1, kv._2))
        spark.read.jdbc(
          fileConfig("url"),
          fileConfig("table"),
          fileConfig("predicates").asInstanceOf[Array[String]], props)
      }
      case "jdbc" => {
        val props = new Properties()
        (fileConfig -- List("url", "table")).foreach(kv => props.put(kv._1, kv._2))
        spark.read.jdbc(
          fileConfig("url"),
          fileConfig("table"),
          props)
      }
      // default option
      case fileType => ss.read.format(fileType).options(fileConfig -- List("fileFormat", "filePath")).load(fileConfig("filePath"))

    }
  }
  
  implicit def anyToString(in:Any) = in.toString

}
