package com.aero.components

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col, explode, size, when, lit}

object FlatJSONDF {
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