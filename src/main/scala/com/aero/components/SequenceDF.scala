package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StructType, LongType, StructField }
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col

object SequenceDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {

    logComponentName("SEQUENCE")
    val sequnceConfig = option.asInstanceOf[Map[String, String]]
    val seqStartVal =
      if (sequnceConfig("startValue").isInstanceOf[String]) {
        sequnceConfig("startValue").asInstanceOf[String].toInt
      } else {
        val seqFromConfig = sequnceConfig("startValue").getOrElse("valueFrom", Map.empty[String, List[String]]).asInstanceOf[Map[String, List[String]]]
        val seqFromConfigList = seqFromConfig.toList(0)
        COLUMN_FUNC(resultMap(seqFromConfigList._1), Map(seqFromConfigList._2(1) -> seqFromConfigList._2(0))).toInt
      }

    val df = sequenceDataFrame(SparkSession.builder().getOrCreate(), resultMap(in.toString),
      seqStartVal,
      sequnceConfig("stepValue").asInstanceOf[String].toInt,
      sequnceConfig("targetColumn").asInstanceOf[String])

    logOutputName(out.toString)
    OTHER_OPTIONS(df, otherOpt, out.toString)

    Map(out.toString() -> df)

  }

  def sequenceDataFrame(
    ss: SparkSession,
    inputDF: DataFrame,
    startVal: Int,
    stepVal: Int,
    targetCol: String): DataFrame = {
    val seqColName = if (targetCol == Nil) "seqNum" else targetCol
    val tempRDD = inputDF.rdd.zipWithIndex.map(x => Row(x._2 :: x._1.toSeq.toList: _*))
    val auxSchema = StructType(Array(StructField(seqColName, LongType, false)))
    val sch = StructType(auxSchema ++ inputDF.schema)
    val outputDF = ss.createDataFrame(tempRDD, sch).withColumn(seqColName, col(seqColName) * stepVal + startVal)
    outputDF
  }

}