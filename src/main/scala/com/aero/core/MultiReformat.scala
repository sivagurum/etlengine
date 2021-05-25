package com.aero.core
 
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Dataset, DataFrame, Column}
import org.apache.spark.sql.expressions.Window

object MultiReformat{
   def main(args: Array[String]) {
     val spark = SparkSession.builder()
     .master("local")
     .appName("Word Count")
     .config("spark.some.config.option", "some-value")
     .getOrCreate()
     
   def mReformatDataFrame(inputDF: DataFrame, mReformat:Map[String,List[(String, String)]], otherOptions: Map[String,String]): Map[String, DataFrame] = {
     val reformatExpr = {x: (String, String) => x._1 + " as " + x._2}
     val dataFrameCols = inputDF.columns.toList
     
     for ((rfmtSeq, reformatCol) <- mReformat) yield {
       
       val reformatHeader = reformatCol.head
       
       val restReformatCols = reformatCol.tail
       
       val patternSuffixAlias = "as \\* \\w+".r
       val suffixAlias = patternSuffixAlias.findFirstIn(reformatHeader._2).getOrElse("").toString()
       val patternPrefixAlias = "as \\w+ \\*".r
       val prefixAlias = patternPrefixAlias.findFirstIn(reformatHeader._2).getOrElse("").toString()
       println(prefixAlias)
       
       val dropColumns = {
         rCols: List[(String,String)] => {
           val dropColExpr = rCols.filter {case (rexpr, rcol) => (rcol == "")}
           val dCols = dropColExpr.map {case (rexpr, rcol) => rexpr}
           (dropColExpr, dCols)
         }
       }
       
       def constructSelectExpr(DFCols: List[String], columnsReformat: List[(String, String)]): List[String] = {
         val dropReformat = dropColumns(columnsReformat)
         val finalTransformExpr = columnsReformat.diff(dropReformat._1)
         val reformatKeyVal = finalTransformExpr.map{case (rexpr, rcol) => (rcol, rexpr)}.toList.toMap
         val existingCols = DFCols.diff(dropReformat._2).map(c => (reformatKeyVal.getOrElse(c, c),c))
         val newCols = finalTransformExpr.map {case (rexpr, rcol) => rcol}.diff(DFCols).map(c => (reformatKeyVal.getOrElse(c, c),c))
         existingCols ++ newCols map {case (rexpr, rcol) => reformatExpr(rexpr, rcol)}      
       }
       
       def applyOtherOptionsDF(outputDF: DataFrame, options: Map[String, String]) = {
         if (otherOptions.getOrElse("cache", "N") == "Y") outputDF.cache()
         if (otherOptions.getOrElse("show", "N") == "N") () else outputDF.show(otherOptions.getOrElse("show","20").toInt, false)
         if (otherOptions.getOrElse("printSchema", "N") == "Y") outputDF.printSchema()
         if (otherOptions.getOrElse("dtypes", "N") == "Y") {
           println("===> Begin dtype ")
           outputDF.dtypes.foreach(println)
           println("<=== End dtype ")
         }
         if (otherOptions.getOrElse("count", "N") == "Y") {
           println("__record count__ " + outputDF.count)
         }
         if (otherOptions.getOrElse("explain", "N") == "Y") outputDF.explain
         if (otherOptions.getOrElse("persist", "N") == "Y") outputDF.persist
         if (otherOptions.getOrElse("dfCountOnColumn", "N") == "N") () 
         else {
           val countCol = otherOptions.getOrElse("dfCountOnColumn","").toString()
           val countExpr = "count(" + countCol + ") as " + "count_" + countCol
           outputDF.selectExpr(countExpr).show
         }
         
         
       }
       
       val outputDF = 
         reformatHeader match {
           case ("*", "*")  => inputDF.selectExpr(constructSelectExpr(dataFrameCols,restReformatCols): _*)
           case ("trim(*)", "*") => {
             val stringTrimCols = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr("trim(" + c + ")",c)))
             inputDF.selectExpr(stringTrimCols: _*).selectExpr(constructSelectExpr(dataFrameCols,restReformatCols): _*)          
           }
           case _ if reformatHeader._2 == suffixAlias => {
             val aliasList = suffixAlias.split(" ")
             val aliasReformatExpr = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr(c ,c + aliasList(2))))  
             val aliasDF = inputDF.selectExpr(aliasReformatExpr: _*)
             aliasDF.selectExpr(constructSelectExpr(aliasDF.columns.toList,restReformatCols): _*)
           }
           case _ if reformatHeader._2 == prefixAlias => {
             val aliasList = prefixAlias.split(" ")
             val aliasReformatExpr = dataFrameCols.diff(dropColumns(restReformatCols)._2).map(c => (reformatExpr(c ,aliasList(1) + c)))  
             val aliasDF = inputDF.selectExpr(aliasReformatExpr: _*)
             aliasDF.selectExpr(constructSelectExpr(aliasDF.columns.toList,restReformatCols): _*)
           }
           case _ => {
             val selectReformatExpr = reformatCol.filter {case (rexpr, rcol) => rcol != ""} map {case (rexpr, rcol) => reformatExpr(rexpr, rcol)}
             inputDF.selectExpr(selectReformatExpr: _*)
           }
         }
       
       applyOtherOptionsDF(outputDF, otherOptions) 
       (rfmtSeq, if (otherOptions.getOrElse("limit", "N") == "N") outputDF else outputDF.limit(otherOptions.getOrElse("limit", "50").toInt))
     }
   }
   
   val dfImdb = spark.read.format("csv").option("header", "true").load("/Users/jagadeeshkumarsellappan/imdb_1000.csv")
   val mReformat = mReformatDataFrame _
     
   /*val reformatImdb = 
     List(("case when star_rating is null then 0 else cast(star_rating as Decimal(3,2)) end", "star_rating"),
         ("title", "title"),
         ("trim(title)", "title"),
         ("cast('1' as String)", "new_column1"),
         ("""cast("yazhini" as String)""", "first_name"),
         ("""cast("jagadeesh" as String)""", "father_name"),
         ("23", "mother_name"),
         ("current_date", "date00"),
         ("current_timestamp", "time00"))*/
//   
//   val reformatImdb = 
//     List(("case when star_rating is null then 0 else cast(star_rating as Int) end", "star_rating"),
//         ("title", ""),
//         ("cast('1' as String)", "new_column1"),
//         ("content_rating", "content_rating_new"),
//         ("genre", "genre"),
//         ("duration", ""),
//         ("actors_list", ""))
   
  val reformatImdb = Map("rfmt01" ->
     List(("trim(*)","*"),
         ("case when star_rating is null then 0 else cast(star_rating as Int) end", "star_rating"),
        ("cast('1' as String)", "new_column1"),
         ("actors_list", "")), 
         "rfmt02" -> List(("*","as master_ *")))
   //val reformatImdb = List(("*","*"))
//   val reformatImdb = 
//     List(("trim(*)","*"),
//       ("case when star_rating is null then 0 else cast(star_rating as Int) end", "star_rating"),
//       ("cast('1' as Decimal(3,2))", "new_column1"),
//       ("duration", ""),
//       ("actors_list", ""))
//  val reformatImdb = 
//     List(("*","as * _master"),
//        ("case when star_rating_master is null then 0 else cast(star_rating_master as Int) end", "star_rating"),
//        ("star_rating_master", ""),
//       ("cast('1' as Decimal(3,2))", "new_column1"),
//       ("duration_master", "duration"),
//       ("actors_list", ""))

     //val reformatImdb = List(("*","as master_ *"))
   //val reformatImdb = List(("*","as * _master"))
   
   val outDF = 
     mReformat(
         dfImdb, 
         reformatImdb, 
         Map("show" -> "10", "show" -> "10", "printSchema" -> "Y")
             )
   outDF("rfmt01").show
   outDF("rfmt02").show
   outDF("rfmt01").explain()
   outDF("rfmt02").explain()
 
   
//          Map(
//             "limit" -> "1",
//             "last" -> "Y",
//             "cache" -> "Y", 
//             "show" -> "10",
//             "printSchema" -> "Y", 
//             "dtypes" -> "Y", 
//             "count" -> "Y",
//             "dfCountOnColumn" -> "star_rating",
//             "explain" -> "Y")
   //outDF.show
   //outDF.printSchema
     
   }
}