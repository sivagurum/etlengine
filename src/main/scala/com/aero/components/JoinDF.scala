package com.aero.components

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.aero.core.EtlFunc._
import com.aero.logging.Logging
import com.aero.utils.implicits._
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col



object JoinDF extends GenericComponent[Any] {

  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    
            val spark = SparkSession.builder().getOrCreate()
            logComponentName("JOIN")
            try {

              val reformatList =
                if (option.getOrElse("reformat", "N") == "N")
                  Nil
                else if (option("reformat").getOrElse("filePath", "N") == "N")
                  option("reformat").asInstanceOf[List[List[String]]].map(x => (x(0), x(1)))
                else {
                  val JoinFileConfig = option.asInstanceOf[Map[String, Map[String, String]]]
                  val filePath = JoinFileConfig("reformat")("filePath").asInstanceOf[String]
                  val fileName = JoinFileConfig("reformat")("fileName").asInstanceOf[String]
                  scala.io.Source.fromFile(filePath + fileName)
                    .getLines().toList
                    .map(x => (x.split("=>")(0).trim, x.split("=>")(1).trim))
                }

              val joinOutList =
                List(
                  out.getOrElse("out", "_").toString,
                  out.getOrElse("new", "_").toString,
                  out.getOrElse("old", "_").toString,
                  out.getOrElse("leftMatch", "_").toString,
                  out.getOrElse("rightMatch", "_").toString)

              val rightJoinKeys =
                if (option.getOrElse("rightKeys", "N") != "N")
                  option("rightKeys").asInstanceOf[List[String]]
                else
                  Nil

              val df = JOINE(
                spark,
                resultMap(in("left")),
                resultMap(in("right")),
                option("leftKeys").asInstanceOf[List[String]],
                rightJoinKeys,
                option.getOrElse("leftSelectCond", "").asInstanceOf[String],
                option.getOrElse("rightSelectCond", "").asInstanceOf[String],
                reformatList,
                option("joinType").asInstanceOf[String],
                joinOutList)

              val mout = joinOutList.zip(df.productIterator.toList).map(x=>x._1 -> x._2.asInstanceOf[DataFrame]).toMap

              val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]

              if (!otherOptKV.isEmpty) {
                for ((k, v) <- otherOptKV) {
                  logOutputName(k.toString)
                  OTHER_OPTIONS(mout(k), v, k)
                }
              }
              mout
            } catch {
              case ex: NoSuchElementException =>
                logError(s"Error in JOIN component, Error =>  ${ex.getMessage}", ex); throw ex
              case ex: RuntimeException =>
                logError(s"Error in JOIN component, Error =>  ${ex.getMessage}", ex); throw ex
            }
          
  }
    def joinDataFrame(
    ss: SparkSession,
    leftDF: DataFrame,
    rightDF: DataFrame,
    leftKeyCols: List[String],
    rightKeyCols: List[String],
    joinType: String,
    joinOutput: List[String]): (DataFrame, DataFrame, DataFrame, DataFrame, DataFrame) = {

    def getJoinOutput(
      leftInputDF: DataFrame,
      rightColsRenamDF: DataFrame,
      joinCondRenamed: Column,
      joinCond: Column,
      typeOfJoin: String,
      jOut: (String, Int)): DataFrame = {

      jOut match {
        case (x, 0) if x != "_" => leftInputDF.join(rightColsRenamDF, joinCondRenamed, typeOfJoin)
        case (x, 1) if x != "_" => leftInputDF.join(rightColsRenamDF, joinCond, "leftanti")
        case (x, 2) if x != "_" => rightDF.join(leftInputDF, joinCond, "leftanti")
        case (x, 3) if x != "_" => leftInputDF.join(rightDF, joinCond, "leftsemi")
        case (x, 4) if x != "_" => rightDF.join(leftInputDF, joinCond, "leftsemi")
        case _ => ss.emptyDataFrame
      }
    }

    val joinOutList =
      if (rightKeyCols.isEmpty) {
        val rightRenameCols = leftDF.columns.toList intersect rightDF.columns.toList diff leftKeyCols
        val rightRenamedDF = rightDF.select(leftKeyCols.map(c => col(c)).union(rightRenameCols.map(c => col(c).alias(c + "_Right"))): _*)
        val joinColsRenamed = leftKeyCols.zip(leftKeyCols).map { case (key1, key2) => leftDF(key1) === rightRenamedDF(key2) }.reduce(_ && _)
        val joinCols = leftKeyCols.zip(leftKeyCols).map { case (key1, key2) => leftDF(key1) === rightDF(key2) }.reduce(_ && _)
        for ((y, i) <- joinOutput.zipWithIndex) yield getJoinOutput(leftDF, rightRenamedDF, joinColsRenamed, joinCols, joinType, (y, i))
      } else {
        val rightRenameCols = leftDF.columns.toList intersect rightDF.columns.toList diff rightKeyCols
        val rightRenamedDF = rightDF.select(rightKeyCols.map(c => col(c)).union(rightRenameCols.map(c => col(c).alias(c + "_Right"))): _*)
        val joinColsRenamed = leftKeyCols.zip(rightKeyCols).map { case (key1, key2) => leftDF(key1) === rightRenamedDF(key2) }.reduce(_ && _)
        val joinCols = leftKeyCols.zip(rightKeyCols).map { case (key1, key2) => leftDF(key1) === rightDF(key2) }.reduce(_ && _)
        for ((y, i) <- joinOutput.zipWithIndex) yield getJoinOutput(leftDF, rightRenamedDF, joinColsRenamed, joinCols, joinType, (y, i))
      }
    (joinOutList(0), joinOutList(1), joinOutList(2), joinOutList(3), joinOutList(4))
  }
}