package com.aero.components

import org.apache.spark.sql.{ DataFrame, SparkSession }
import com.aero.core.EtlFunc._
import org.apache.spark.sql.functions.{ lit }
import com.aero.utils.implicits._
import com.aero.core.aeroDriver._

object SCD2DF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {

    logComponentName("SCD2")
    val df =
      scd2DataFrame(
        spark,
        resultMap(in("left")),
        resultMap(in("right")),
        option("joinKeys").asInstanceOf[List[String]],
        (option("startDateColumn"), resolveParamteres(option("startDate"))), (option("endDateColumn"), resolveParamteres(option("endDate"))))
    logOutputName(out.toString)
    OTHER_OPTIONS(df, otherOpt, out.toString)
    Map(out.toString() -> df)

  }
  def scd2DataFrame(
    ss: SparkSession,
    deltaDF: DataFrame,
    masterDF: DataFrame,
    joinKeyCols: List[String],
    scd2Start: (String, String),
    scd2End: (String, String)): DataFrame = {

    val filterCondStr = scd2End._1 + " = " + '"' + scd2End._2 + '"'

    val (activeMasterDF, inactiveMasterDF) = FILTER(masterDF, Map("filterCond" -> filterCondStr, "deSelect" -> "Y"))

    //activeMasterDF.cache
    val (_, newDF, oldDF, deltaMatchDF, masterMatchDF) =
      JOIN(ss, deltaDF, activeMasterDF, joinKeyCols, Nil, "inner", List("_", "Y", "Y", "Y", "Y"))

    val masterMatchRawDF = masterMatchDF.drop(scd2Start._1).drop(scd2End._1)

    val deltaSeqCol = deltaMatchDF.columns.toList

    val (deltaUpdateDF, masterMatchOldDF, noChangeMasterDF) =
      DIFF(deltaMatchDF.selectExpr(deltaSeqCol: _*), masterMatchRawDF.selectExpr(deltaSeqCol: _*))

    val newSCD2DF = newDF.withColumn(scd2Start._1, lit(scd2Start._2)).withColumn(scd2End._1, lit(scd2End._2))

    val deltaUpdateSCD2DF = deltaUpdateDF.withColumn(scd2Start._1, lit(scd2Start._2)).withColumn(scd2End._1, lit(scd2End._2))

    //val (_, _, _, _, getSCD2masterMatchOldDF1) =
    //  JOIN(ss, masterMatchOldDF, activeMasterDF, joinKeyCols, Nil, "inner", List("_", "_", "_", "_", "Y"))
    val getSCD2masterMatchOldDF = activeMasterDF.join(masterMatchOldDF, joinKeyCols, "leftsemi")

    val updateSCD2masterMatchOldDF = getSCD2masterMatchOldDF.withColumn(scd2End._1, lit(scd2Start._2))

    //val (_, _, _, _, getSCD2noChangeMasterDF) =
    //  JOIN(ss, noChangeMasterDF, activeMasterDF, joinKeyCols, Nil, "inner", List("_", "_", "_", "_", "Y"))
    val getSCD2noChangeMasterDF = activeMasterDF.join(noChangeMasterDF, joinKeyCols, "leftsemi")

    val seqSCD2Cols = inactiveMasterDF.columns.toList

    val outputDF = UNION(
      Seq(
        inactiveMasterDF.selectExpr(seqSCD2Cols: _*),
        oldDF.selectExpr(seqSCD2Cols: _*),
        newSCD2DF.selectExpr(seqSCD2Cols: _*),
        deltaUpdateSCD2DF.selectExpr(seqSCD2Cols: _*),
        updateSCD2masterMatchOldDF.selectExpr(seqSCD2Cols: _*),
        getSCD2noChangeMasterDF.selectExpr(seqSCD2Cols: _*)))
    //activeMasterDF.unpersist()
    outputDF
  }

}