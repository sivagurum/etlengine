package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.expressions.Window
import com.aero.core.EtlFunc._

object ScanDF {
    def scanDataFrame(
    inputDF: DataFrame,
    keyCol: List[String],
    orderCol: List[String],
    FuncColCond: (String, String, String, String),
    dropCumCol: Char): DataFrame = {

    val cumPartition = Window.partitionBy(keyCol.map(col(_)): _*).orderBy(orderCol.map(col(_)): _*)
    val scanCol = if (FuncColCond._4 > " ") FuncColCond._4 else FuncColCond._1 + FuncColCond._2
    val scanString = FuncColCond._2 + " " + FuncColCond._3 + " " + scanCol
    val cumDF = CUMULATIVE(inputDF, keyCol, orderCol, (FuncColCond._1, FuncColCond._2, FuncColCond._4), dropCumCol)
    val filterExpr = scanCol + "Flag" + " is null or " + scanCol + " = true"
    REFORMAT(cumDF, List(("*", "*"), (scanString, scanCol + "Flag"))).filter(filterExpr).drop(scanCol + "Flag")
  }
}