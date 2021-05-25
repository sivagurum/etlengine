package com.aero.custom.udc

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import com.aero.components.UDCComponent
import org.apache.spark.sql.functions.lit
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._

object AddColumn extends UDCComponent {

  override def init(in: Map[String, DataFrame], out: List[String], spark: SparkSession) = {
    out.zip(in.map(_._2.withColumn("extra_column", lit("my column")))).toMap
  }

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    logComponentName("ADD COLUMN: UDC")
    val mout = out.asInstanceOf[List[String]].zip(resultMap.map(_._2.withColumn("extra_column", lit("my column")))).toMap
    //resultMap.map(_._2.withColumn("extra_column", lit("my column"))).foreach(x=>println(x.schema))
    val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]
    if (!otherOptKV.isEmpty) {
      for ((k, v) <- otherOptKV) {
        logOutputName(k.toString)
        OTHER_OPTIONS(mout(k), v, k)
      }
    }
    mout
  }
}

