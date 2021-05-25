package com.aero.core
 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Dataset, DataFrame, Column}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
 
object GenerateRec{
   def main(args: Array[String]) {
     val spark = SparkSession.builder()
     .master("local")
     .appName("Word Count")
     .config("spark.some.config.option", "some-value")
     .getOrCreate()
//     val df = spark.read.json("/Users/jagadeeshkumarsellappan/movies_collection.json")
     //df.withColumn("reviews", explode(col("reviews")))
     //.select(List("reviews.date.$date", "title", "year", "reviews.name","reviews.rating", "reviews.comment").map(col(_)): _*).show
     //df.printSchema
     import spark.implicits._
     case class ImdbSchema(star_rating: Double, title: String, content_rating:String, genre: String, duration: Int, actors_list: String)
     
     val sch = StructType(Array(StructField("star_rating",DoubleType,true), StructField("title",StringType,true), 
         StructField("content_rating",StringType,true), 
         StructField("genre",StringType,true), StructField("duration",IntegerType,true), 
         StructField("actors_list",StringType,true)))
         
     val dfImdb = spark.read.format("csv")
     .option("header", "true")
     .schema(sch)
     .load("/Users/jagadeeshkumarsellappan/imdb_1000.csv")

     val total_count = dfImdb.repartition(5, col("genre")).agg(count("star_rating"))
     total_count.explain()
     total_count.show
     println("count --->"+ dfImdb.count)
     
     

     

     
   }
}