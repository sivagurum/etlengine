package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.logging.Logging
import com.aero.utils.implicits._
import com.aero.core.EtlFunc._

object DQValidateDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {

    logComponentName("DQ")
    val df = dqValidatorDataFrame(
      resultMap(in.toString),
      option("requiredDQInputs").asInstanceOf[List[(String)]],
      option("stringDQInputs").asInstanceOf[List[(String, Int, Int)]],
      option("intDQInputs").asInstanceOf[List[(String, String)]],
      option("doubleDQInputs").asInstanceOf[List[(String, String)]],
      option("datetimeDQInputs").asInstanceOf[List[(String, String)]])

    val mout = Map(out.asInstanceOf[List[String]](0) -> df._1, out.asInstanceOf[List[String]](1) -> df._2)
    val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]

    if (!otherOptKV.isEmpty) {
      for ((k, v) <- otherOptKV) {
        logOutputName(k.toString)
        OTHER_OPTIONS(mout(k), v, k)
      }
    }
    mout

  }
  def dqValidatorDataFrame(
    inputDF: DataFrame,
    requiredDQInputs: List[String],
    stringDQInputs: List[(String, Int, Int)],
    intDQInputs: List[(String, String)],
    doubleDQInputs: List[(String, String)],
    datetimeDQInputs: List[(String, String)]): (DataFrame, DataFrame) = {

    val (dqCols, dqSelectExpr) =
      if (requiredDQInputs.nonEmpty && stringDQInputs.isEmpty && intDQInputs.isEmpty && doubleDQInputs.isEmpty && datetimeDQInputs.isEmpty) {
        (requiredDQInputs, constructSelExpr(requiredDQInputs.map(c => isColumnRequired(c))))
      } else {
        val stringDQCols = stringDQInputs.map { case (c, min, max) => c } toList
        val stringDQSelect = stringDQInputs.map { case (c, min, max) => stringDQ(c, min, max, requiredDQInputs.contains(c)) }

        val intDQCols = intDQInputs.map { case (c, dtype) => c } toList
        val intDQSelect = intDQInputs.map { case (c, dtype) => intDQ(c, dtype, requiredDQInputs.contains(c)) }

        val doubleDQCols = doubleDQInputs.map { case (c, colSize) => c } toList
        val doubleDQSelect = doubleDQInputs.map { case (c, colSize) => doubleDQ(c, colSize, requiredDQInputs.contains(c)) }

        val datetimeDQCols = datetimeDQInputs.map { case (c, dtype) => c } toList
        val datetimeDQSelect = datetimeDQInputs.map { case (c, dtype) => datetimeDQ(c, dtype, requiredDQInputs.contains(c)) }

        val dqCols = stringDQCols ++ intDQCols ++ doubleDQCols ++ datetimeDQCols
        val dqSelectExpr = constructSelExpr(stringDQSelect ++ intDQSelect ++ doubleDQSelect ++ datetimeDQSelect)
        (dqCols, dqSelectExpr)
      }
    getValidRejectDF(inputDF, dqCols, dqSelectExpr)
  }
  val constructSelExpr: List[(String, String)] => List[String] =
    dqConst => {
      dqConst match {
        case (head :: tail) => List(head._1, head._2) ++ constructSelExpr(tail)
        case _ => List()
      }
    }

  val isColumnRequired = {
    dqString: String =>
      {
        val colName = dqString
        val caseExpr =
          "case when " + colName + " is not null then '*'" +
            " else " + "'" + "++ invalid " + colName + "'" +
            " end as " + "string" + colName + "dqFlag"
        (colName, caseExpr)
      }
  }
  val getValidRejectDF = {
    (inputDF: DataFrame, dqCols: List[String], dqExpr: List[String]) =>
      {

        val dfCols = inputDF.columns.toList
        val nonDQCols = dfCols diff dqCols
        val dqDF = inputDF.selectExpr(nonDQCols ++ dqExpr: _*)
        val dqFlagCols = dqDF.columns.toList.filter(_.endsWith("dqFlag"))
        val validCond = dqFlagCols.map(_ + """ = "*"""").mkString(" and ")
        val concateExpr = dqFlagCols.mkString("translate(concat(", ",", "),'*','') as rejectReason")
        val validFlagDF = dqDF.filter(validCond)
        val rejectFlagDF = dqDF.except(validFlagDF)

        (validFlagDF.selectExpr(dfCols: _*), rejectFlagDF.selectExpr(dfCols :+ concateExpr: _*))
      }
  }
  val stringDQ = {
    dqString: (String, Int, Int, Boolean) =>
      {
        val colName = dqString._1
        val minLen = dqString._2
        val maxLen = dqString._3
        val isColumnRequired = dqString._4

        val caseExpr =
          if (isColumnRequired) {
            "case when " + colName + " is not null and " +
              "(length(trim(" + colName + ")) >= " + minLen + " and length(trim(" + colName + ")) <= " + maxLen + ") then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          } else {
            "case when " + colName + " is null then '*'" +
              " when (length(trim(" + colName + ")) >= " + minLen + " and length(trim(" + colName + ")) <= " + maxLen + ") then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + "string" + colName + "dqFlag"
          }
        (colName, caseExpr)
      }
  }

  val intDQ = {
    dqString: (String, String, Boolean) =>
      {
        val colName = dqString._1
        val dataType = dqString._2
        val isColumnRequired = dqString._3

        val caseExpr =
          if (isColumnRequired) {
            "case when " + colName + " is not null and cast(" + colName + ") as " + dataType + ") is not null then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          } else {
            "case when " + colName + " is null then '*'" +
              " when cast(trim(" + colName + ") as " + dataType + ") is not null then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          }
        (colName, caseExpr)
      }
  }

  val doubleDQ = {
    dqString: (String, String, Boolean) =>
      {
        val colName = dqString._1
        val colAttr = dqString._2
        val isColumnRequired = dqString._3

        val caseExpr =
          if (isColumnRequired) {
            "case when " + colName + " is not null and cast(" + colName + " as Decimal(" + colAttr + ")) is not null then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          } else {
            "case when " + colName + " is null then '*'" +
              " when cast(" + colName + " as Decimal(" + colAttr + ")) is not null then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          }
        (colName, caseExpr)
      }
  }

  val datetimeDQ = {
    dqString: (String, String, Boolean) =>
      {
        val colName = dqString._1
        val dataORtimestamp = dqString._2
        val isColumnRequired = dqString._3

        val caseExpr =
          if (isColumnRequired) {
            "case when " + colName + " is not null and cast(" + colName + " as " + dataORtimestamp + ") is not null then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          } else {
            "case when " + colName + " is null then '*'" +
              " when cast(" + colName + " as " + dataORtimestamp + ") is not null then '*'" +
              " else " + "'" + "++ invalid " + colName + "'" +
              " end as " + colName + "dqFlag"
          }
        (colName, caseExpr)
      }
  }
}