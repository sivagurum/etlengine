package com.aero.components

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{row_number, lit, col}
import org.apache.spark.sql.expressions.Window
import com.aero.utils.implicits._
import com.aero.core.EtlFunc._

object SequenceSortDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    
            logComponentName("SEQUENCE")

            val df = sequenceDataFrameWindow(
              resultMap(in.toString),
              option("startValue").toInt,
              option("stepValue").toInt,
              option.getOrElse("targetColumn", "-"),
              option.getOrElse("sortColumns", Nil).asInstanceOf[List[String]],
              option.getOrElse("partitionColumns", Nil).asInstanceOf[List[String]])

            logOutputName(out.toString)
            OTHER_OPTIONS(df, otherOpt, out)

            Map (out.toString() -> df)
          
  }
    def sequenceDataFrameWindow(
    inputDF: DataFrame,
    startVal: Int,
    stepVal: Int,
    targetCol: String,
    colOrd: List[String],
    partitionCol: List[String]): DataFrame = {

    val seqColName = if (targetCol == "-") "seqNum" else targetCol

    val output =
      (colOrd, partitionCol) match {
        case (Nil, Nil) =>
          inputDF.withColumn("tempCol", lit(1))
            .withColumn(seqColName, row_number() over Window.partitionBy().orderBy("tempCol"))
            .drop("tempCol")
            .withColumn(seqColName, (col(seqColName) - 1) * stepVal + startVal)
        case (x, Nil) =>
          inputDF.withColumn(seqColName, row_number() over Window.partitionBy().orderBy(x.map(col(_)): _*))
            .withColumn(seqColName, (col(seqColName) - 1) * stepVal + startVal)
        case (Nil, x) =>
          inputDF.withColumn("tempCol", lit(1))
            .withColumn(seqColName, row_number() over Window.partitionBy(x.map(col(_)): _*).orderBy("tempCol"))
            .drop("tempCol")
            .withColumn(seqColName, (col(seqColName) - 1) * stepVal + startVal)
        case (x, y) =>
          inputDF.withColumn(seqColName, row_number() over Window.partitionBy(y.map(col(_)): _*)
            .orderBy(x.map(col(_)): _*))
            .withColumn(seqColName, (col(seqColName) - 1) * stepVal + startVal)
      }
    output.selectExpr((output.columns.toList.last +: output.columns.toList.init): _*)
  }
}