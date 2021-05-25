package com.aero.core

import EtlFunc._
import CustomFunc._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Encoder, Dataset, DataFrame}
import org.apache.spark.sql.Column
import org.apache.log4j.{Level, Logger}

object MainTest{
  def main(args: Array[String]) {
    val spark = SparkSession
            .builder()
            .master("local")
            .appName("etl")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()
            
    val rootLogger = Logger.getRootLogger() 
    rootLogger.setLevel(Level.ERROR)
 
    val inputConfig = 
      if (args.length > 0) {
        //args.mkString(('"'.toString * 3).toString()," ",('"'.toString * 3).toString()).split(";").map(x => (x.split("->")(0).trim(), x.split("->")(1).trim())).toMap
        //args.mkString(" ").split(";").map(x => (x.split("->")(0).trim(), x.split("->")(1).trim().mkString('"'.toString(), "", '"'.toString()))).toMap
        //args.map(x => (x.split("->")(0).trim(), x.split("->")(1).trim())).toMap
        args.map(x => x.split("->")).map(x => (x(0).trim, x(1).trim)).toMap
      }
      else {
        Map("" -> "")
      }
    inputConfig.foreach(println)
    
    val fileName = "/Users/jagadeeshkumarsellappan/Downloads/SalesJan2009.csv"
    val noneOtherOptions = Map("" -> "")
    //val fileName = "/Users/jagadeeshkumarsellappan/imdb_1000.csv"
    
    val saleFileConfig = Map("filePath" -> fileName)
    
    val csvConfig = 
      Map("fileFormat" -> "csv",
          "header" -> "true", 
          "inferSchema" -> "true", 
          "delimiter" -> ",")  
          
    import spark.implicits._
    val saleFileDF = READ(spark,saleFileConfig ++ csvConfig) 
    saleFileDF.limit(3).show
    
    /*//val colName = LIMIT(saleFileDF, "1").rdd.collect.mkString("").replace("[", "").replace("]", "").split(",")
    val colName = LIMIT(saleFileDF, "1").na.fill("NULL0").na.fill(99999).selectExpr("concat(" +  saleFileDF.columns.mkString(", ',',") + ") as value")
    .map(r => r.getString(0)).collect.toList

    
    colName.flatMap(x => x.split(",")).foreach(println)
    val colList = colName.flatMap(x => x.split(",")).zipWithIndex.map { x => if(x._1 == "NULL0") "col_" + x._2.toString else x._1 }
    FILTER(saleFileDF,Map("filterCond" -> "skip(1)"))._1.toDF(colList: _*).show
    
    //val cols1 = spark.read.textFile(fileName).limit(1).select("value").map(r => r.getString(0)).collect.toList 
    //cols1.foreach(println)
    System.exit(-1)*/
    val stateFileConfig = Map("filePath" -> "/Users/jagadeeshkumarsellappan/Downloads/US_State_code.csv")
    val stateFileDF = READ(spark,stateFileConfig ++ csvConfig)
    
    val saleRfmt = 
     List(
         ("Product", "Product"),
         ("Name", "Name"),
         ("Country", "Country"),
         ("State", "StateCode"),
         ("Price", "")
         )
   
   val rfmtOut = REFORMAT(saleFileDF, saleRfmt)
   //rfmtOut.show
 
   val salesFilterCond = Map("filterCond" -> "Country = 'United States'", "deSelect" -> "Y")
   
   val (filterDF, restDF) = FILTER(rfmtOut, salesFilterCond)
   //filterDF.show 
   //restDF.show 
   
//   val saleRfmt1 = 
//     scala.io.Source.fromFile(inputConfig("rfmtFilePath") + "/" + inputConfig("rfmtFile"))
//     .getLines().toList
//     .map(x => (x.split("=>")(0).trim, x.split("=>")(1).trim))
////     .map {case (x,y) => 
////       (if (x.indexOf("p->") == 0) inputConfig.getOrElse((x.substring(3).trim), x) 
////       else if(x.indexOf("p->") > 0) {
////         inputConfig.map {case(kParam, pValue) => if (x.indexOf("p-> " + kParam) > 0) x.replaceAll("p-> "+ kParam, inputConfig(kParam)) else null}
////         .filter(_ != null).mkString
////       }
////       else x,
////       y)
////     }
   
    def resolveParams(rfmtExp: String): String = {
      val parmPattern = "\\$\\'(\\w+)\\'".r
      parmPattern.findFirstIn(rfmtExp) match {
       case None => rfmtExp
       case x => {
         val parmPatternField = "\\w+".r
         val expParamField = x.get
         println(expParamField)
         val pField = parmPatternField.findFirstIn(x.get)
         val expField = pField.get
         println(expField)
         println(inputConfig(expField))
         rfmtExp.replace(expParamField, inputConfig(expField))
         }
       }
     }
   
   val saleRfmt1N = 
     scala.io.Source.fromFile(inputConfig("rfmtFilePath") + "/" + inputConfig("rfmtFile"))
     .getLines().toList
     .map(x => (x.split("=>")(0).trim, x.split("=>")(1).trim))
     .map {case (x,y) => (resolveParams(x), y)}
   

    saleRfmt1N.foreach(println)
//     List(
//         ("*", "*"),
//         ("Name", ""),
//         ("Country", ""),
//         ("State", "")
//         )
   
   val rfmtOut1 = REFORMAT(saleFileDF.withColumn("testCol", lit("true")), saleRfmt1N)
   rfmtOut1.show
   rfmtOut1.printSchema()
   System.exit(-1)
   val salesFIlterUK = Map("filterCond" -> "Country = 'United Kingdom'")
   val (filterDFuk, _) = FILTER(rfmtOut, salesFIlterUK)
   filterDFuk.show 
   
   val mRout = 
     MREFORMAT(saleFileDF, 
         Map("US" -> saleRfmt, "UK" -> saleRfmt),
         Map("US" -> "Country = 'United States'", "UK" -> "Country = 'United Kingdom'"))
   mRout("UK").show
   mRout("US").show
   
   val mUSDF = mRout("US")
   
   val lookOut = LOOKUP(mRout("US"), stateFileDF, "State", Seq("StateCode"))
   lookOut.show
   
   val pivotOut = 
     PIVOT(lookOut.limit(20), List("Product"), "State", List(("coalesce(sum(Price),3)",  "sum_price"), ("count(Price)", "")))
   pivotOut.show
   
   val cumOut = 
     CUMULATIVE(lookOut.limit(20), List("Product"), List("State"), ("lag", "Price", ""), 'N')
   cumOut.show
   
   val nullD = NULLDROP(cumOut,("", List("lagPrice")))
   nullD.show
   //System.exit(-1)
   val cumRout = REFORMAT(cumOut, List(("*", "*"), ("Price = lagPrice", "priceFlag")))
   cumRout.show
   val scanOut = 
     SCAN(lookOut.limit(20), List("Product"), List("State"), ("lag", "Price", "<>",""), 'N')
   scanOut.show
   //val scanOut = 
   //  SCAN(lookOut.limit(20), List("Product"), List("Name"), "Price", 'N')
   val sqlS = """select * from lookOut where State = 'Virginia'"""
   val sqlOut = SQL(spark, Map("lookOut" -> lookOut), sqlS) 
   sqlOut.show
   sqlOut.explain()
       
   val sqlS1 = """select mUS.*, stateDF.State from mUS, stateDF where mUS.StateCode = stateDF.StateCode"""
   
   val sqlEOut = SQL(spark, Map("mUS" -> mUSDF, "stateDF" -> stateFileDF), sqlS1)
   sqlEOut.show
   
   
  }
}