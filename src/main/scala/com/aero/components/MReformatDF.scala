package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.sql.types.StringType

object MReformatDF {
  def mReformatDataFrame(
    inputDF: DataFrame,
    mReformat: Map[String, List[(String, String)]],
    mReformatCond: Map[String, String]): Map[String, DataFrame] = {

    val dataFrameCols = inputDF.columns.toList

    for ((rfmtkey, reformatCol) <- mReformat) yield {

      val reformatHeader = reformatCol.head
      val restReformatCols = reformatCol.tail

      val patternSuffixAlias = "as \\* \\w+".r
      val suffixAlias = patternSuffixAlias.findFirstIn(reformatHeader._2).getOrElse("").toString()
      val patternPrefixAlias = "as \\w+ \\*".r
      val prefixAlias = patternPrefixAlias.findFirstIn(reformatHeader._2).getOrElse("").toString()

      val outputDF =
        reformatHeader match {
          case ("*", "*") => inputDF.selectExpr(constructSelectExpr(dataFrameCols, restReformatCols): _*)
          case ("trim(*)", "*") => {
            val dfColDtype = inputDF.dtypes.toMap
            val stringTrimCols =
              dataFrameCols.diff(dropColumns(restReformatCols)._2)
                .map(c => (reformatExpr(if (dfColDtype(c) == StringType) s"trim(${c})" else c, c)))
            if (mReformatCond.getOrElse(rfmtkey, "N") == "N")
              inputDF.selectExpr(stringTrimCols: _*).selectExpr(constructSelectExpr(dataFrameCols, restReformatCols): _*)
            else
              inputDF.filter(mReformatCond(rfmtkey)).selectExpr(stringTrimCols: _*).selectExpr(constructSelectExpr(dataFrameCols, restReformatCols): _*)
          }
          case _ if reformatHeader._2 == suffixAlias => {
            val aliasList = suffixAlias.split(" ")
            val aliasReformatExpr = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr(c, c + aliasList(2))))
            val aliasDF =
              if (mReformatCond.getOrElse(rfmtkey, "N") == "N")
                inputDF.selectExpr(aliasReformatExpr: _*)
              else
                inputDF.filter(mReformatCond(rfmtkey)).selectExpr(aliasReformatExpr: _*)
            aliasDF.selectExpr(constructSelectExpr(aliasDF.columns.toList, restReformatCols): _*)
          }
          case _ if reformatHeader._2 == prefixAlias => {
            val aliasList = prefixAlias.split(" ")
            val aliasReformatExpr = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr(c, aliasList(1) + c)))
            val aliasDF =
              if (mReformatCond.getOrElse(rfmtkey, "N") == "N")
                inputDF.selectExpr(aliasReformatExpr: _*)
              else
                inputDF.filter(mReformatCond(rfmtkey)).selectExpr(aliasReformatExpr: _*)
            aliasDF.selectExpr(constructSelectExpr(aliasDF.columns.toList, restReformatCols): _*)
          }
          case _ => {
            val selectReformatExpr = reformatCol.filter { case (rexpr, rcol) => rcol != "" } map { case (rexpr, rcol) => reformatExpr(rexpr, rcol) }
            if (mReformatCond.getOrElse(rfmtkey, "N") == "N")
              inputDF.selectExpr(selectReformatExpr: _*)
            else
              inputDF.filter(mReformatCond(rfmtkey)).selectExpr(selectReformatExpr: _*)
          }
        }
      (rfmtkey, outputDF)
    }
  }
  val reformatExpr = { x: (String, String) => if (x._1 == x._2) x._1 else x._1 + " as " + x._2 }
  def constructSelectExpr(DFCols: List[String], columnsReformat: List[(String, String)]): List[String] = {
    val dropReformat = dropColumns(columnsReformat)
    val finalTransformExpr = columnsReformat.diff(dropReformat._1)
    val reformatKeyVal = finalTransformExpr.map { case (rexpr, rcol) => (rcol, rexpr) }.toList.toMap
    val existingCols = DFCols.diff(dropReformat._2).map(c => (reformatKeyVal.getOrElse(c, c), c))
    val newCols = finalTransformExpr.map { case (rexpr, rcol) => rcol }.diff(DFCols).map(c => (reformatKeyVal.getOrElse(c, c), c))
    existingCols ++ newCols map { case (rexpr, rcol) => reformatExpr(rexpr, rcol) }
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