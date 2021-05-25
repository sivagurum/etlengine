package com.aero.core
 
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Dataset, DataFrame, Column}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
 
object Expld{
   def main(args: Array[String]) {
     
     val conf = new SparkConf().setAppName("WordCount").setMaster("local")
     val sc = new SparkContext(conf)
     val sqlContext = new SQLContext(sc)
     val a1 = """{"a1":[{"k1": {"comments": ["a","b","c"]}}, {"k1": {"comments": ["1","2","3"]}}]}"""
     val a1DF = sqlContext.read.json(sc.parallelize(Seq(a1)))
     val winSpec = Window.partitionBy("com").orderBy("dum").rowsBetween(Window.unboundedPreceding, Window.currentRow)
     val SeqN = row_number().over(winSpec).alias("seqNum")
     
     a1DF.withColumn("com", explode(col("a1.k1.comments")))
     .withColumn("dum", lit(1))
     .select(explode(col("com")), SeqN - 1 as "seq").show
   }
}
