package com.aero.components

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.aero.core.EtlFunc._
import org.apache.spark.sql.functions.{lit}
import com.aero.utils.implicits._
import com.aero.core.aeroDriver._

object SCD1DF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    

            logComponentName("SCD1")

            val df =
              scd1DataFrame(
                SparkSession.builder().getOrCreate(),
                resultMap(in("left")),
                resultMap(in("right")),
                option("joinKeys").asInstanceOf[List[String]],
                (option("startDateColumn"), resolveParamteres(option("startDate"))))

            logOutputName(out.toString)
            OTHER_OPTIONS(df, otherOpt, out)
            Map(out.toString() -> df)
          
  }
  
    def scd1DataFrame(
    ss: SparkSession,
    deltaDF: DataFrame,
    masterDF: DataFrame,
    joinKeyCols: List[String],
    scd1Date: (String, String)): DataFrame = {

    val (_, newDF, oldDF, deltaMatchDF, masterMatchDF) =
      JOIN(ss, deltaDF, masterDF, joinKeyCols, Nil, "inner", List("_", "Y", "Y", "Y", "Y"))

    val newSCD1DF = newDF.withColumn(scd1Date._1, lit(scd1Date._2))

    val masterMatchRawDF = masterMatchDF.drop(scd1Date._1)

    val masterSeqCol = masterMatchRawDF.columns.toList

    val (deltaUpdateDF, _, noChangeMasterDF) =
      DIFF(deltaMatchDF.selectExpr(masterSeqCol: _*), masterMatchRawDF.selectExpr(masterSeqCol: _*))

    val deltaUpdateSCD1DF = deltaUpdateDF.withColumn(scd1Date._1, lit(scd1Date._2))

    //val (_, _, _, _, getSCD1noChangeMasterDF) =
    //  JOIN(ss, noChangeMasterDF, masterMatchDF, joinKeyCols, Nil, "inner", List("_", "_", "_", "_", "Y"))

    val getSCD1noChangeMasterDF = masterMatchDF.join(noChangeMasterDF, joinKeyCols, "leftsemi")

    val seqSCD1Cols = masterDF.columns.toList

    UNION(
      Seq(
        newSCD1DF.selectExpr(seqSCD1Cols: _*),
        oldDF.selectExpr(seqSCD1Cols: _*),
        deltaUpdateSCD1DF.selectExpr(seqSCD1Cols: _*),
        getSCD1noChangeMasterDF.selectExpr(seqSCD1Cols: _*)))
  }
  
}