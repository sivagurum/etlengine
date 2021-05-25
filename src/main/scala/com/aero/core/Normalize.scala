package com.aero.core
 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Dataset, DataFrame, Column}
import org.apache.spark.sql.expressions.Window
 
object Normalize{
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
     
//     val reviewSchema = 
//       (new StructType)
//       .add("comment", StringType)
//       .add("date",(new StructType)
//           .add("$date", StringType))
//       .add("name", StringType)
//      .add("rating", DoubleType)
       
      
       //val schemaJson = df.dtypes.toMap
//       val schemaJson1 = StructType(StructField("comment",StringType,true), 
//           StructField("date",StructType(StructField("$date",StringType,true)),true), 
//           StructField("name",StringType,true), StructField("rating",DoubleType,true))
//      
//       val reviewSchema1 = StructType(StructField("comment",StringType,true), 
//           StructField("date",StructType(StructField("$date",StringType,true)),true), 
//           StructField("name",StringType,true), 
//           StructField("rating",DoubleType,true))
//           StructType(Array(StructField("firstName",StringType,true),
//               StructField("lastName",StringType,true),
//               StructField("age",IntegerType,true)))
//       df.columns.foreach(println)  
//       val dfNull = 
//         df.withColumn("reviews", explode(when(col("reviews").isNotNull, col("reviews"))
//             .otherwise(array(lit(null).cast(reviewSchema)))))
//             .select(List("substr(title,1", "year as year1",  "cast(reviews.rating as Int) as rating","reviews.name","reviews.comment").map(expr(_)): _*)
//       dfNull.show
     
   def sequenceDataFrame(inputDF: DataFrame, startVal: Int, stepVal: Int, targetCol: String,colOrd: String*): DataFrame = {
       val seqColName = if (targetCol == Nil) "seqNum" else targetCol
       colOrd match {
         case Nil => 
           inputDF.withColumn("tempCol", lit(1))
           .withColumn(seqColName, row_number() over Window.orderBy("tempCol"))
           .drop("tempCol").withColumn(seqColName, (col(seqColName) - 1) * stepVal + startVal)
         case x => 
           inputDF.withColumn(seqColName, row_number() over Window.orderBy(x.map(col(_)):_*))
           .withColumn(seqColName, (col(seqColName) - 1) * stepVal + startVal)
       }
     }
     val dfImdb = spark.read.format("csv").option("header", "true").load("/Users/jagadeeshkumarsellappan/imdb_1000.csv")
     val sequenceDataFrameFn = sequenceDataFrame _
     
     //val outDF = sequenceDataFrameFn(dfImdb,Nil) 
     val colOrd = Seq("star_rating")
     val outDF = sequenceDataFrameFn(dfImdb,100,-2,"seqNumNew",colOrd)
     outDF.explain()
     outDF.show(200)
     
   }
}