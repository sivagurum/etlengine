package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.storage.StorageLevel._

object CacheDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {

    logComponentName("STORAGE")
    storageDataFrame(option.map(entry => (resultMap(entry._1) -> entry._2)))
    val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]

    if (!otherOptKV.isEmpty) {
      for ((k, v) <- otherOptKV) {
        logOutputName(k.toString)
        OTHER_OPTIONS(resultMap(k), v, k)
      }
    }
    resultMap

  }
  def storageDataFrame(storageOptions: Map[DataFrame, String]) = {
    for ((inputDF, sOpt) <- storageOptions) {
      sOpt match {
        case "cache" => inputDF.cache()
        case "persist" => inputDF.persist()
        case "unpersist" => inputDF.unpersist()
        case sOpt => inputDF.persist(fromString(sOpt))
      }
    }
  }
}