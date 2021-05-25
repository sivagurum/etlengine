package com.aero.components

import org.apache.spark.sql.types.{ DataType, StructType }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ array, col, explode, size, when, lit }
import com.aero.utils.implicits._
import com.aero.core.EtlFunc._
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import net.liftweb.json.parse

object NormalizeDF extends GenericComponent[Any] {
  def execute(resultMap: Map[String, DataFrame], in: Any, out: Any, option: Any, otherOpt: Any): Map[String, DataFrame] = {
    
    implicit val formats = DefaultFormats
    logComponentName("NORMALIZE")

    val normalizeSChema =
      if (option.getOrElse("schema", "N") != "N") {
        val schemaJson = write(option("schema"))
        Map("schema" -> schemaJson.mkString)
      } else Map.empty[String, String]
    val normalizeKey = Map("normalizeKey" -> option("normalizeKey"))

    val df = normalizeDataFrame(
      resultMap(in.toString),
      normalizeKey ++ normalizeSChema,
      option("select").asInstanceOf[List[String]])

    logOutputName(out.toString)
    OTHER_OPTIONS(df, otherOpt, out.toString())
    Map (out.toString() -> df)

  }

  def normalizeDataFrame(inputDF: DataFrame, normalizeConfig: Map[String, String], selCol: List[String]): DataFrame = {
    //println(inputDF.schema)
    val flatCol = normalizeConfig("normalizeKey")

    //inputDF.printSchema()
    if (normalizeConfig.getOrElse("schema", "N") == "N")
      inputDF.withColumn(normalizeConfig("normalizeKey").split('.').last, explode(col(normalizeConfig("normalizeKey")))).selectExpr(selCol: _*)
    else {
      inputDF.withColumn(normalizeConfig("normalizeKey").split('.').last, explode(col(normalizeConfig("normalizeKey")))).printSchema()
      println(DataType.fromJson(normalizeConfig("schema")).asInstanceOf[StructType])
      //System.exit(-1)
      val flattenSchema = DataType.fromJson(normalizeConfig("schema")).asInstanceOf[StructType]
      inputDF.withColumn(flatCol.split('.').last, explode(array(lit(null).cast(flattenSchema)))).printSchema()
      //inputDF.withColumn(flatCol.split('.').last, explode(when((col(flatCol).isNotNull), col(flatCol))
      //.otherwise(array(lit(null).cast(flattenSchema))))).printSchema()
      inputDF.withColumn(flatCol.split('.').last, explode(when((col(flatCol).isNotNull and size(col(flatCol)).gt(0)), col(flatCol))
        .otherwise(array(lit(null).cast(flattenSchema))))).selectExpr(selCol: _*)
    }
  }

  def normalizeDataFrameE(
    inputDF: DataFrame,
    rootSelCol: List[String],
    flattenCol: List[String],
    norSchema: Map[String, String]): DataFrame = {

    //flattens schema at field level
    def flattenFieldLevel(
      jDF: DataFrame,
      rootCol: List[String],
      flatCol: String,
      flattenSchema: StructType): DataFrame = {

      jDF.selectExpr(rootCol: _*)
        .withColumn(
          flatCol.split('.').last, explode(when((col(flatCol).isNotNull and size(col(flatCol)).gt(0)), col(flatCol))
            .otherwise(array(lit(null).cast(flattenSchema)))))
    }

    flattenCol match {
      case fCol :: Nil => {
        flattenFieldLevel(inputDF, rootSelCol, fCol, DataType.fromJson(norSchema.getOrElse(fCol, "")).asInstanceOf[StructType])
      }
      case fCol :: tCol => {
        normalizeDataFrameE(
          flattenFieldLevel(inputDF, rootSelCol, fCol, DataType.fromJson(norSchema.getOrElse(fCol, "")).asInstanceOf[StructType]),
          List(fCol),
          tCol,
          norSchema)
      }
      case _ => inputDF.limit(0)
    }
  }

}