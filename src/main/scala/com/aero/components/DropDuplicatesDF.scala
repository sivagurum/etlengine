package com.aero.components

import org.apache.spark.sql.DataFrame
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._

object DropDuplicatesDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    
            logComponentName("DROPDUP")
            val df = removeDuplicates(resultMap(in.toString), option("keys").asInstanceOf[List[String]], option("keep").asInstanceOf[String])
            //otherOptions
            logOutputName(out.toString)
            OTHER_OPTIONS(df, otherOpt, out.toString())
            Map(out.toString() -> df)
          
  }
  
    def removeDuplicates(inputDF: DataFrame, dupCols: List[String], dupOption: String): DataFrame = {
    dupCols match {
      case _ if dupOption == "unique" => {
        val uniqueDF = inputDF.dropDuplicates(dupCols).selectExpr(inputDF.columns.toList.map(c => c + " as " + c): _*)
        val duplicateDF = inputDF.except(uniqueDF)
        inputDF.join(duplicateDF, dupCols, "leftanti").selectExpr(inputDF.columns.toList: _*).show
        inputDF.join(duplicateDF, dupCols, "leftanti").selectExpr(inputDF.columns.toList: _*)
      }
      case _ if dupOption == "first" => inputDF.dropDuplicates(dupCols).selectExpr(inputDF.columns.toList: _*)
      case _ if dupOption == "last" => {
        val removeDupLastCond = (for (gcols <- inputDF.columns.toList diff (dupCols)) yield (dupOption, gcols, gcols))
        GROUPBY(inputDF, dupCols, removeDupLastCond).selectExpr(inputDF.columns.toList: _*)
      }
      case Nil => inputDF.dropDuplicates().selectExpr(inputDF.columns.toList: _*)
      case _ => inputDF.dropDuplicates(dupCols).selectExpr(inputDF.columns.toList: _*)
    }
  }
  
}