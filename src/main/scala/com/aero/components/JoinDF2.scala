package com.aero.components

import org.apache.spark.sql.{ DataFrame, SparkSession }
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object JoinDF2 {
  def joinDataFrameE(
    ss: SparkSession,
    leftDF: DataFrame,
    rightDF: DataFrame,
    leftKeyCols: List[String],
    rightKeyCols: List[String],
    leftDFSelCond: String,
    rightDFSelCond: String,
    joinReformat: List[(String, String)],
    joinType: String,
    joinOutput: List[String]): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

    val dfMap =
      Map(if (leftDFSelCond == Nil || leftDFSelCond.trim == "") "leftDF" -> leftDF else "leftDF" -> leftDF.filter(leftDFSelCond)) ++
        Map(if (rightDFSelCond == Nil || rightDFSelCond.trim == "") "rightDF" -> rightDF else "rightDF" -> rightDF.filter(rightDFSelCond))

    val joinCols =
      leftKeyCols.zip(if (rightKeyCols.isEmpty) leftKeyCols else rightKeyCols)
        .map { case (key1, key2) => leftDF(key1) === rightDF(key2) }.reduce(_ && _)

    val joinOutList =
      for (jOut <- joinOutput.zipWithIndex) yield {
        jOut match {
          case (x, 0) if x != "_" => {
            val rightRenamedDF = dfMap("rightDF").selectExpr(dfMap("rightDF").columns.toList.map(rCol => s"${rCol} as ${rCol}_Right"): _*)
            val joinColsRenamed =
              leftKeyCols.zip(if (rightKeyCols.isEmpty) leftKeyCols.map(_ + "_Right") else rightKeyCols.map(_ + "_Right"))
                .map { case (key1, key2) => dfMap("leftDF")(key1) === rightRenamedDF(key2) }.reduce(_ && _)
            dfMap("leftDF").join(rightRenamedDF, joinColsRenamed, joinType)
          }
          case (x, 1) if x != "_" => dfMap("leftDF").join(dfMap("rightDF"), joinCols, "leftanti")
          case (x, 2) if x != "_" => dfMap("rightDF").join(dfMap("leftDF"), joinCols, "leftanti")
          case (x, 3) if x != "_" => dfMap("leftDF").join(dfMap("rightDF"), joinCols, "leftsemi")
          case (x, 4) if x != "_" => dfMap("rightDF").join(dfMap("leftDF"), joinCols, "leftsemi")
          case _ => ss.emptyDataFrame
        }
      }

    val joinRfmtOut =
      if (joinReformat == Nil)
        joinOutList(0)
      else {
        joinReformat match {
          case head :: tail if head == ("left.*", "*") => {
            val dropCols = dropColumns(tail)._2
            REFORMAT(joinOutList(0), (leftDF.columns.toList diff (dropCols ++ tail.map(x => x._2))).map(lCol => (lCol, lCol)) ++ tail)
          }
          case head :: tail if head == ("right.*", "*") => {
            val dropCols = dropColumns(tail)._2
            REFORMAT(joinOutList(0), (rightDF.columns.toList.map(_ + "_Right") diff (dropCols ++ tail.map(x => x._2))).map(rCol => (rCol, rCol)) ++ tail)
          }
          case _ => REFORMAT(joinOutList(0), joinReformat)
        }
      }
    (joinRfmtOut, joinOutList(1), joinOutList(2), joinOutList(3), joinOutList(4))
  }

  val dropColumns = {
    rCols: List[(String, String)] =>
      {
        val dropColExpr = rCols.filter { case (rexpr, rcol) => (rcol == "" || rcol == "-" || rcol == null) }
        val dCols = dropColExpr.map { case (rexpr, rcol) => rexpr }
        dropColExpr.foreach(println)
        (dropColExpr, dCols)
      }
  }
}