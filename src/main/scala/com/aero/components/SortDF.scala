package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.sql.functions.{ asc_nulls_first, asc_nulls_last, desc_nulls_first, desc_nulls_last, asc, desc }
import com.aero.core.aeroDriver._

object SortDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {

    try {
      logComponentName("SORT")
      val df =
        SORT(
          resultMap(in.toString),
          option("sortKeys").asInstanceOf[List[List[String]]].map(x => (resolveParamteres(x(0)), resolveParamteres(x(1)))))

      //Storage level
      val storageLevel = resolveParamteres(option.getOrElse("storageLevel", "N"))
      if (storageLevel != "N") STORAGE(Map(df -> storageLevel))
      //otherOptions
      logOutputName(out.toString)
      OTHER_OPTIONS(df, otherOpt, out)
      Map(out.toString() -> df)
    } catch {
      case ex: NoSuchElementException =>
        log.error(s"Error in SORT component, Error =>  ${ex.getMessage}", ex); throw ex
      case ex: RuntimeException =>
        log.error(s"Error in SORT component, Error =>  ${ex.getMessage}", ex); throw ex
    }

  }

  def sortDataFrame(inputDF: DataFrame, sortCondList: List[(String, String)]): DataFrame = {
    //validate sort columns
    val columnsDF = inputDF.columns.toList
    val isSortColumnExist = sortCondList.map(x => (x._1 -> columnsDF.contains(x._1))).filter(x => x._2 == false)
    if (!isSortColumnExist.isEmpty) {
      val sortColNotFound = isSortColumnExist.map(x => x._1).mkString(",")
      EXIT(inputDF, Map("errorMessage" -> s"sort key column(s): '${sortColNotFound}' not found", "exitRC" -> "-1"))
    }
    val sortCond =
      sortCondList.map {
        case (sCols, sOrd) => sOrd match {
          case "asc" => asc(sCols)
          case "desc" => desc(sCols)
          case "asc_nulls_first" => asc_nulls_first(sCols)
          case "asc_nulls_last" => asc_nulls_last(sCols)
          case "desc_nulls_first" => desc_nulls_first(sCols)
          case "desc_nulls_last" => desc_nulls_last(sCols)
        }
      }
    inputDF.orderBy(sortCond: _*)
  }

}