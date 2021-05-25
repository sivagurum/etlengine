package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.sql.types.StringType

object ReformatDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {

    logComponentName("REFORMAT")
    val reformatConfig = option.asInstanceOf[Map[String, Map[String, String]]]

    val reformatExp =
      option.keySet.toSeq match {
        case key if key.contains("reformatCol") && key.contains("reformatFile") => {
          val errorMsg =
            s"invalid reformat - cannot be provided both 'reformatCol' and 'reformatFile' in '  ' \n ' ___ ${option} ___ '"
          logError(errorMsg)
          System.exit(-1)
          List.empty[(String, String)]
        }
        case key if key.contains("reformatCol") => reformatConfig("reformatCol").asInstanceOf[List[List[String]]].map(x => (x(0), x(1)))
        case key if key.contains("reformatFile") => {
          val filePath = reformatConfig("reformatFile")("filePath")
          val fileName = reformatConfig("reformatFile")("fileName")
          scala.io.Source.fromFile(filePath + fileName)
            .getLines().toList
            .filter(!_.startsWith("#"))
            .map(x => (x.split("=>")(0).trim, x.split("=>")(1).trim))
        }
        case _ => {
          val errorMsg =
            s"invalid reformat - either 'reformatCol' or 'reformatFile' needs to be provided in ' ' \n ' ___ ${option} ___ '"
          logError(errorMsg)
          System.exit(-1)
          List.empty[(String, String)]
        }
      }

    val df = reformatDataFrame(resultMap(in.toString), reformatExp)
    logOutputName(out.toString)
    OTHER_OPTIONS(df, otherOpt, out.toString())

    Map(out.toString() -> df)

  }
  def reformatDataFrame(
    inputDF: DataFrame,
    reformatCol: List[(String, String)]): DataFrame = {
    logInfo("___XFR details___"+reformatCol)
    val reformatHeader = reformatCol.head
    val dataFrameCols = inputDF.columns.toList
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
          val dropC = dropColumns(restReformatCols)._2
          val stringTrimCols =
            dataFrameCols.map(c => (reformatExpr(if (dfColDtype(c) == StringType || !dropC.contains(c)) s"trim(${c})" else c, c)))
          inputDF.selectExpr(stringTrimCols: _*).selectExpr(constructSelectExpr(dataFrameCols, restReformatCols): _*)
        }
        case _ if reformatHeader._2 == suffixAlias => {
          val aliasList = suffixAlias.split(" ")
          val aliasReformatExpr = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr(c, c + aliasList(2))))
          val aliasDF = inputDF.selectExpr(aliasReformatExpr: _*)
          aliasDF.selectExpr(constructSelectExpr(aliasDF.columns.toList, restReformatCols): _*)
        }
        case _ if reformatHeader._2 == prefixAlias => {
          val aliasList = prefixAlias.split(" ")
          val aliasReformatExpr = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr(c, aliasList(1) + c)))
          val aliasDF = inputDF.selectExpr(aliasReformatExpr: _*)
          aliasDF.selectExpr(constructSelectExpr(aliasDF.columns.toList, restReformatCols): _*)
        }
        case _ => {
          val selectReformatExpr =
            reformatCol.filter { case (rexpr, rcol) => (rcol != "" && rcol != "-") } map { case (rexpr, rcol) => reformatExpr(rexpr, rcol) }
          inputDF.selectExpr(selectReformatExpr: _*)
        }
      }
    outputDF
  }

  val reformatExpr = { x: (String, String) => if (x._1 == x._2) x._1 else x._1 + " as " + x._2 }

  val dropColumns = {
    rCols: List[(String, String)] =>
      {
        val dropColExpr = rCols.filter { case (rexpr, rcol) => (rcol == "" || rcol == "-" || rcol == null) }
        val dCols = dropColExpr.map { case (rexpr, rcol) => rexpr }
        dropColExpr.foreach(println)
        (dropColExpr, dCols)
      }
  }
  def constructSelectExpr(DFCols: List[String], columnsReformat: List[(String, String)]): List[String] = {
    val dropReformat = dropColumns(columnsReformat)
    val finalTransformExpr = columnsReformat.diff(dropReformat._1)
    val reformatKeyVal = finalTransformExpr.map { case (rexpr, rcol) => (rcol, rexpr) }.toList.toMap
    val existingCols = DFCols.diff(dropReformat._2).map(c => (reformatKeyVal.getOrElse(c, c), c))
    val newCols = finalTransformExpr.map { case (rexpr, rcol) => rcol }.diff(DFCols).map(c => (reformatKeyVal.getOrElse(c, c), c))
    existingCols ++ newCols map { case (rexpr, rcol) => reformatExpr(rexpr, rcol) }
  }
}