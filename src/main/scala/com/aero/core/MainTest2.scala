package com.aero.core

import EtlFunc._
import CustomFunc._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, Dataset, DataFrame}
import org.apache.spark.sql.Column
import org.apache.log4j.{Level, Logger}

object MainTest2{
  def main(args: Array[String]) {
    val spark = SparkSession
            .builder()
            .master("local")
            .appName("etl")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()
            
    val rootLogger = Logger.getRootLogger() 
    rootLogger.setLevel(Level.ERROR)

    val noneOtherOptions = Map("" -> "")
    
    val masterFile = Map("filePath" -> "/Users/jagadeeshkumarsellappan/Documents/SPARK_POC/scd2_emp_appn_master.json")
    val deltaFile = Map("filePath" -> "/Users/jagadeeshkumarsellappan/Documents/SPARK_POC/scd2_emp_appn_delta.json")
    
    val fConfig = Map("fileFormat" -> "json")
  
    val masterDF = READ(spark,fConfig ++ masterFile) 
    masterDF.show
    println(READ)
    val deltaDF = READ(spark, fConfig ++ deltaFile)
    deltaDF.show
    println(READ)
    val scd2DF = 
      SCD2(spark, deltaDF, masterDF, List("appn_id", "seq_nm"), ("snap_dt", """"08-23-2017""""), ("snap_end_dt", """"12-31-2100""""))
    scd2DF.show
    
    val scd1masterFile = Map("filePath" -> "/Users/jagadeeshkumarsellappan/Documents/SPARK_POC/scd1_emp_appn_master.json")
    val scd1deltaFile = Map("filePath" -> "/Users/jagadeeshkumarsellappan/Documents/SPARK_POC/scd1_emp_appn_delta.json")
    
    val scd1masterDF = READ(spark,fConfig ++ scd1masterFile) 
    scd1masterDF.show
    
    val scd1deltaDF = READ(spark, fConfig ++ scd1deltaFile)
    scd1deltaDF.show
    
    val scd1DF = 
      SCD1(spark, scd1deltaDF, scd1masterDF, List("appn_id", "seq_nm"), ("snap_dt", """"08-23-2017""""))
    scd1DF.show
  }
}