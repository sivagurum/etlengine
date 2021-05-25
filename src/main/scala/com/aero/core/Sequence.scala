package com.aero.core
 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Dataset, DataFrame, Column, Row}
import org.apache.spark.sql.expressions.Window
 
object Sequence{
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
           .withColumn(seqColName, row_number() over Window.partitionBy().orderBy("tempCol"))
           .drop("tempCol").withColumn(seqColName, (col(seqColName) - 1) * stepVal + startVal)
         case x => 
           inputDF.withColumn(seqColName, row_number() over Window.partitionBy().orderBy(x.map(col(_)):_*))
           .withColumn(seqColName, (col(seqColName) - 1) * stepVal + startVal)
       }
     }
     
     
     def sequenceDataFrame01(inputDF: DataFrame, startVal: Int, stepVal: Int, targetCol: String): DataFrame = {
       val seqColName = if (targetCol == Nil) "seqNum" else targetCol
       val tempRDD = inputDF.repartition(10).rdd.zipWithIndex.map(x => Row(x._2 :: x._1.toSeq.toList: _*))
       val auxSchema = StructType(Array(StructField(seqColName, LongType, false)))
       val sch = StructType(auxSchema ++ inputDF.schema) 
        
       spark.createDataFrame(tempRDD, sch).withColumn(seqColName, col(seqColName) * stepVal + startVal)
     }
     
     
     val dfImdb = spark.read.format("csv").option("header", "true").load("/Users/jagadeeshkumarsellappan/imdb_1000.csv")
     val sequenceDataFrameFn = sequenceDataFrame _
     val sequenceDataFrameFn01 = sequenceDataFrame01 _
     
      
     val colOrd = Seq("star_rating")
     
//     println("source" + dfImdb.rdd.getNumPartitions)
//     val outDF = sequenceDataFrameFn(dfImdb,100,-2,"seqNumNew",colOrd) 
//     println("num of partitions" + outDF.rdd.getNumPartitions)
//     outDF.explain
//     outDF.show(100)
     
     
     val outDF1 = sequenceDataFrameFn(dfImdb,1,1,"seqNum",Nil)
     println("num of partitions" + outDF1.rdd.getNumPartitions)
     outDF1.explain
     //outDF1.show(100)
     //outDF1.repartition(20).write.csv("/Users/jagadeeshkumarsellappan/imdb_1000_out2.csv")
     val b: StructType = dfImdb.schema
     println(b)
     //case class MyC(b: dfImdb.schema, c: Long)
     //dfImdb.rdd.zipWithIndex.map{case(Row(a),c: Long) => MyC(a,c)}
     
//     def zi(a: (Row, Long)) = {
//       val b = a._1.toSeq.toList 
//       a._2 :: b
//     }
//     val targetCol = "seqNum"
     //val ardd = dfImdb.repartition(10).rdd.zipWithIndex.map {tuple => Row(tuple._2, tuple._1(0),tuple._1(1),tuple._1(2),tuple._1(3),tuple._1(4),tuple._1(5))}
     //val dfImdb = spark.read.format("csv").option("header", "true").load("/Users/jagadeeshkumarsellappan/imdb_1000.csv")
//     val ardd = dfImdb.repartition(10).rdd.zipWithIndex.map(x => Row(x._2 :: x._1.toSeq.toList: _*))
//     val auxSchema = StructType(Array(StructField(targetCol, LongType, false)))
//     val sch = StructType(auxSchema++dfImdb.schema) 
//     sch.foreach(println)
     //println(sch)
         
     val seqDF = sequenceDataFrameFn01(dfImdb,1,1,"seqNumNew")
     seqDF.explain
     
     val seqDF1 = seqDF.repartition(10)
     val seqDF2 = seqDF.repartition(10).coalesce(5)
     seqDF.show(20)
     seqDF1.show(20)
     seqDF2.show(100)
     seqDF.explain()
     seqDF1.explain()
     seqDF2.explain()
     //spark.createDataFrame(ardd, sch).write.csv("/Users/jagadeeshkumarsellappan/imdb_1000_out3.csv")
     
     //val dfImdb1 = dfImdb.repartition(5).sort(desc("genre")).withColumn("seqNum", monotonically_increasing_id() + 1)
     //val dfImdb2 = dfImdb1.repartition(20)
     //dfImdb1.show
     //dfImdb2.show
     //dfImdb2.explain
     
     //dfImdb.repartition(5).withColumn("seqNum", monotonicallyIncreasingId()).show
     
   }
}