package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.utils.implicits._

object ExitDF extends GenericComponent[Any] {
  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
          logInfo(s"_____EXIT_____")
          val df = exitDataFrame(
            resultMap(in.toString),
            option)
            Map.empty[String,DataFrame]
        }
  def exitDataFrame(inputDF: DataFrame, exitCondMap: Map[String, String]) = {
    val exitKeysList = exitCondMap.keys.toList
    if (exitKeysList.contains("condition")) {
      exitCondMap("condition") match {
        case "empty" => {
          if (inputDF.limit(1).count() == 0) {
            logError(exitCondMap("errorMessage"))
            System.exit(exitCondMap("exitRC").toInt)
          }
        }
        case "notEmpty" => {
          if (inputDF.limit(1).count() == 1) {
            logError(exitCondMap("errorMessage"))
            System.exit(exitCondMap("exitRC").toInt)
          }
        }
        case _ => ()
      }
    } else if (exitKeysList.contains("thresholdLimit")) {
      exitCondMap("thresholdLimit") match {
        case thl if thl.forall(_.isDigit) => {
          if (inputDF.limit(thl.toInt + 1).count() > thl.toInt) {
            logError(exitCondMap("errorMessage"))
            System.exit(exitCondMap("exitRC").toInt)
          }
        }
        case _ => ()
      }
    } else {
      logError(exitCondMap("errorMessage"))
      System.exit(exitCondMap("exitRC").toInt)
    }
  }
}