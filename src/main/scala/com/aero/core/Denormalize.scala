package com.aero.core
 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Dataset, DataFrame, Column}
import org.apache.spark.sql.expressions.Window
 
object Denormalize{
   def main(args: Array[String]) {
     val spark = SparkSession.builder()
     .master("local")
     .appName("Word Count")
     .config("spark.some.config.option", "some-value")
     .getOrCreate()

  //val dfImdb = spark.read.format("csv").option("header", "true").load("/Users/jagadeeshkumarsellappan/imdb_1000.csv")
  val balFile = spark.read.format("csv").option("header", "true").load("/Users/jagadeeshkumarsellappan/bal_file.csv")
  //dfImdb.show(200,false)
       
  balFile.groupBy("id").pivot("month").agg(max("bal") as "bal", first("date") as "date").show
     
   }
}