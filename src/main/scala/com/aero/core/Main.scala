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

object Main{
  def main(args: Array[String]) {
    val spark = SparkSession
            .builder()
            .master("local")
            .appName("etl")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()
            
    val rootLogger = Logger.getRootLogger() 
    rootLogger.setLevel(Level.ERROR)
    
//    val inputConfig = args.mkString(" ")    
//    println(inputConfig)
//    
//    val keyValueConfig = inputConfig.split(";").map(_.split("->")).map { case Array(k, v)  => (k.trim, v.trim)}.toMap   
//    
//    val primKeyColumns = keyValueConfig("primaryKeyCol").split(",").toList
//    println("primKeyColumns ->", primKeyColumns)
    
    //val readFile = spark.read.parquet _
    
    //val masterDF = readFile(keyValueConfig("masterFile"))
    val masterDF = 
      spark.read.format("csv")
      .options(Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> ","))  
      .load("/Users/jagadeeshkumarsellappan/imdb_1000.csv")
    
//    val getFirstCond = (for (colsDF <- masterDF.columns.toList) yield ("first", colsDF, colsDF)) 
//    getFirstLastDF(masterDF, Nil, getFirstCond).show
  val READ = com.aero.components.ReadDF.readDataFrame _
  val WRITE = com.aero.components.WriteDF.writeDataFrame _
  val GROUPBY = com.aero.components.GroupByDF.groupByDataFrame _
  val FILTER = com.aero.components.FilterDF.filterDataFrame _
  val SORT = com.aero.components.SortDF.sortDataFrame _
  val JOIN = com.aero.components.JoinDF.joinDataFrame _
  val DEDUP = com.aero.components.DedupDF.dedupDataFrame _
  val DROP_DUPLICATES = com.aero.components.DropDuplicatesDF.removeDuplicates _
  val REFORMAT = com.aero.components.ReformatDF.reformatDataFrame _
  val MREFORMAT = com.aero.components.MReformatDF.mReformatDataFrame _
  val DIFF = com.aero.components.DiffDF.diffDataFrame _
  val UNION = com.aero.components.UnionDF.unionDataFrame _
  val LOOKUP = com.aero.components.LookupDF.lookupDataFrame _
  val SEQUENCE = com.aero.components.SequenceDF.sequenceDataFrame _
  val PARTITION = com.aero.components.PartitionDF.partitionDataFrame _
  val STORAGE = com.aero.components.CacheDF.storageDataFrame _
  val DQVALIDATOR = com.aero.components.DQValidateDF.dqValidatorDataFrame _
  val LIMIT = com.aero.components.LimitDF.limitDataFrame _
  val PIVOT = com.aero.components.PivotDF.pivotDataFrame _
  val CUMULATIVE = com.aero.components.CumulativeDF.cumulativeDataFrame _
  val SQL = com.aero.components.SQLDF.sqlDataFrameE _
  val SCAN = com.aero.components.ScanDF.scanDataFrame _
  val NORMALIZE = com.aero.components.NormalizeDF.normalizeDataFrame _
  val NORMALIZEE, FLAT_JSON = com.aero.components.FlatJSONDF.normalizeDataFrameE _
  val SORT_PARTITION = com.aero.components.SortPartitionDF.sortPartitionDataFrame _
  val NULLDROP = com.aero.components.NulldropDF.nullDropDataFrame _
  val SCD1 = com.aero.components.SCD1DF.scd1DataFrame _
  val SCD2 = com.aero.components.SCD2DF.scd2DataFrame _
  
  val OTHER_OPTIONS = applyOtherOptionsDF _
  val EXIT = com.aero.components.ExitDF.exitDataFrame _

  val MINUS = com.aero.components.MinusDF.minusDataFrame _
  val INTERSECT = com.aero.components.IntersectDF.intersectDataFrame _
  val MFILTER = com.aero.components.MultiFilterDF.multiFilterDataFrame _
  val RENAME_COLUMNS = com.aero.components.RenameColumnsDF.renameColumnsDataFrame _
  val JOINE = com.aero.components.JoinDF2.joinDataFrameE _
  val SEQUENCE_WITH_SORT = com.aero.components.SequenceSortDF.sequenceDataFrameWindow _
  val LOOKUPE = com.aero.components.LookupDF.lookupDataFrameE _
  val COLUMN_FUNC = com.aero.components.ColumnFunctionsDF.columnFunctionDataFrame _
  val GROUPBYE = com.aero.components.GroupByDF2.groupByDataFrameE _
  val SELECT = com.aero.components.SelectDF.selectDataFrame _
  val DROP = com.aero.components.DropDF.dropDataFrame _
  val REGUDF = registerUDF _
  val CUSTCOMP = customComp _
    
    val sortOut =  SORT(masterDF, List(("star_rating", "desc_nulls_first")))
    sortOut.show()
    System.exit(0)
    val grpOutDF = 
      GROUPBY(masterDF,
          List("genre"),
          List(("max","*","max_ *"), ("min","star_rating","min_star_rating")))
    grpOutDF.show
    
    val (validDF, rejectDF) = 
      //DQVALIDATOR(masterDF,Nil,Nil)
      DQVALIDATOR(
          masterDF,
         // Nil,
          List("title", "star_rating"),
       //   Nil,
          List(("genre", 1, 9),("title", 0, 50)), 
          Nil,
        //  List(("duration", "SmallInt")), 
          Nil,
       //   List(("star_rating", "2,1")),
          Nil
          )
      
      validDF.show(false)
      rejectDF.show(false)
      
    val filterCond = "head"
    val deSelect = 'N'
    
    val fOut = FILTER(masterDF, Map("filterCond" -> "head", "deSelect" -> "Y"))

     
  //  dfFilter.show
  //  dfFilter.explain
    
    spark.udf.register("genArray", genArray)
    spark.udf.register("isShortMovie", isShortMovie _)
    spark.udf.register("isMovieShort", isMovieShort)
    
    val reformatImdb01 = 
     List(("trim(*)","*"),
   //      ("case when star_rating is null then 0 else cast(star_rating as Int) end", "star_rating"),
   //      ("cast('1' as String)", "new_column1"),
         ("actors_list", ""))
         
  val reformatImdb02 = 
     List(
        ("case when star_rating is null then 0 else cast(star_rating as Int) end", "star_rating"),
         ("cast('1' as String)", "new_column1"),
         ("size(genArray(actors_list))", "actors_list"),
         ("isMovieShort(duration)", "isMovieShort"))
  val outDFOptions = 
    Map("limit" -> "","last" -> "N", "show" -> "10","printSchema" -> "Y", "dtypes" -> "Y", "count" -> "Y","dfCountOnColumn" -> "star_rating","explain" -> "Y")     
  val outDF = 
      REFORMAT(
         masterDF, 
         reformatImdb02
         )
  outDF.show(false)
  outDF.printSchema
  
//   println("___ sequence __")
//   val seqDF = 
//     sequence(
//         spark,
//         outDF("R002"),
//         1,
//         1,
//         "seqCol",
//         Map("limit" -> "25",
//             "cache" -> "Y", 
//             "show" -> "26",
//             "printSchema" -> "Y", 
//             "dtypes" -> "Y", 
//             "count" -> "Y",
//             "dfCountOnColumn" -> "star_rating",
//             "explain" -> "Y")
//         )
//    seqDF.show(26)    
   /* 
    //masterDF.show 
      
   //filter function
    val filterCond = """data_obj_id like "U:t%""""
    val deSelect = 'N'
    
    val dfFilterList = filterDataFrame _
    dfFilterList(masterDF, filterCond, "Y")(0).show
     
  //  dfFilter.show
  //  dfFilter.explain
    
    val filterCond2 = """data_obj_id like "U:t%""""
    val deSelect2 = 'N'
    
    val dfFilter2List = filterDataFrame(masterDF, filterCond2, "Y")
    dfFilter2List(0).show
    dfFilter2List(1).show
    dfFilter2List(1).explain
    
    //println(dfFilter2)
   
    val deltaDF = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(keyValueConfig("deltaFile"))
    
    val joinC = List("data_qlty_data_obj_srgt_id")
    val joinType = "inner"
    val numOfOutputs = 3
    
    val joinOutDF = joinDataFrame(dfFilterList(masterDF, filterCond, "Y")(0), deltaDF, joinC, joinType, numOfOutputs)
    
    joinOutDF.foreach(_.show)
    //joinOutDF.foreach(_.explain)
    //joinOutDF(1).cache()
    
                            
    
    
    val rfmtDF = reformatDataFrame(joinOutDF(1), rfmtCond, reformtCols)
    rfmtDF.show()
    rfmtDF.printSchema
    rfmtDF.explain
    
  //  dfFilter2.show
  //  dfFilter2.explain
  //  if (deSelect == 'Y') { 
  //    val dfSelectFilter = filterDataFrame(masterDF,dfFilter)  
  //    dfSelectFilter.show
  //    }
   // sort function
    val sortCond = List(("desc","data_qlty_data_obj_srgt_id"),("asc","data_obj_id"))
    
    
    val dfSort = sortDataFrame(joinOutDF(1), sortCond) 
    dfSort.show
    
    val unionDF = unionDataFrame(joinOutDF(0),joinOutDF(0)) 
    unionDF.show
    // dedup function
  //  val deDupC = List("row_end_ts") 
  //  val dupOption: String = "last"
    
    //val (dfDeDup, dfDuplicates) = removeDuplicates(dfSort, deDupC, dupOption, 'Y')
    //dfDeDup.show()
    //dfDuplicates.show()
    //Aggregate functions 
    //val groupC = List("user_tlbar_id", "rec_mod_ts") 
    //val aggCond = List(("max","role_scurt_lvl_num", "max_role_scurt_lvl_num")) 
    
    val groupDF = groupByDataFrame(dfSort, groupC, aggCond)
    //groupDF.show()
    
    //First or Last
    
    //val firstOrLast = "first"
    //val aggCols = dfSort.columns.toList
    
    //val groupDF1 = groupByDataFrame(dfSort, groupC, aggCols,firstOrLast)
    //groupDF1.show
    //println(groupDF.explain)
    //println(groupDF1.explain)*/
    /*val groupC = List("user_tlbar_id", "rec_mod_ts") 
    val aggCond = List(("max","role_scurt_lvl_num", "max_role_scurt_lvl_num")) 
    
    val groupDF = groupByDataFrame(masterDF, List("user_tlbar_id", "rec_mod_ts"), List(("max","role_scurt_lvl_num", "max_role_scurt_lvl_num")))
    val groupByoutDF = groupByDataFrame(masterDF, List("statecode","county"), List(("max","role_scurt_lvl_num","max_role_scurt_lvl_num"),("min","role_scurt_lvl_num","min_role_scurt_lvl_num")))*/
    
  
  //  val fields = StructType(StructField("star_rating", DoubleType, nullable = true) ::
  //      StructField("title", StringType, nullable = true) ::
  //      StructField("content_rating", StringType, nullable = true) ::
  //      StructField("genre", StringType, nullable = true) ::
  //      StructField("duration", IntegerType, nullable = true) ::
  //      StructField("actors_list", ArrayType(StringType, containsNull = true)) :: Nil)
        
/*    val deltaDF =  spark.read.json("/home/hduser/test_files/sample.json")
  //    spark.read.format("csv").schema(fields)
  //    .options(Map("header" -> "true", "delimiter" -> ","))  
  //    .load("/home/hduser/test_files/imdb_1000_1.csv")
    deltaDF.show
    deltaDF.printSchema
    
    val rfmtCond = List(("""explode(image.height)""", "imageH"))*/
                      /*  ( """current_timestamp()""", "current_time"),
                        ("""case when point_granularity >= 1 then "equal" 
                                 when point_granularity <= 10 then "low" 
                                 else "high" end""", "some_ind"),       
                         ("""trim(statecode)""",  "statecode"),
                         ("""substr(line,1,4)""",  "asline"),
                         ("""date_format("2100-12-31", "yyyymmdd")""", "snap_end_dt"),
                         ("""format_number(policyID, 3)""", "sampleColumn")) */
                         
   //val rfmtCond = List(())                    
  /*  val rfmtCond = List((""""2100-12-31"""", "snap_end_dt"),
                        ("""data_obj_type""", "data_obj_type_rename"),
                        ("""cast(12 as Double)""", "sor_id_new"))*/
                        
//    val reformtCols = List("select")
//                         
//    val arrowReformat = reformatDataFrame _
//    println(arrowReformat)
//    val rfmtDF = arrowReformat(deltaDF, rfmtCond, reformtCols)
//    
//    rfmtDF.show()
//    rfmtDF.printSchema
//    rfmtDF.explain
//    deltaDF.show
//    
//    println("---------------- normalize ------------------")
//    val union = unionDataFrame _
//    val normalize = normalizeDataFrame _
//    
//    val nDFTemp = normalize(deltaDF, "image", List("id", "type", "thumbnail"), List("height", "url", "width"))
//    nDFTemp.show
//    val rfmtCond1 = List(("""thumbnail.height""", "theight"),
//                        ("""thumbnail.url""", "turl"),
//                        ("""thumbnail.width""", "twidth"),
//                        ("""cast(id as Double)""", "idDouble"))
//    val nDFTemp1 = arrowReformat(nDFTemp, rfmtCond1, List("select", "id", "idDouble"))
//    nDFTemp1.show
    
  //  deltaDF.withColumn("height", explode(deltaDF("image.height")))
  //  .withColumn("url", explode(deltaDF("image.url")))
  //  .withColumn("width", explode(deltaDF("image.width"))).drop("image").select(
  //  
  //  val arrowJoin = joinDataFrame _
    //println(arrowJoin)
    
    //val dedup = deDupDataFrame _
    
  //  val rfmtCond1 = List(("""explode(image.height)""", "imageNor2"))
  //   val rfmtDF1 = arrowReformat(deltaDF, rfmtCond, Nil)
  //   rfmtDF1.show
  //  
    /*val reformtCols1 = List("select", "policyID","policyID_new", "current_time", "some_ind", "statecode", "statecodeTrim", "snap_end_dt", "line", "asline")
    val rfmtDF1 = reformatDataFrame(rfmtDF, Nil, reformtCols1)
    rfmtDF1.show()
    
    rfmtDF1.explain
    */
  /*  val joinL = List("LNKEY")
    val joinR = List("LNKEY")
    val joinType = "inner"
    
    
    val (matchJoinDF, leftOnlyDF, rightOnlyDF, leftMatchDF, rightMatchDF) = joinDataFrame(
        spark, deltaDF, masterDF, joinL, joinR, joinType, List("innerDF", "_", "_","leftMatch", "rightMatch"))
    
    matchJoinDF.show*/
    //leftOnlyDF.show
    //rightOnlyDF.show
    //leftMatchDF.show
    //rightMatchDF.show
    //joinOutDF.foreach(_.explain)
    //joinOutDF(1).cache()
    
    /*println("----------------------> old code")
    val joinC1 = List("data_qlty_data_obj_srgt_id")
    val joinType1 = "inner"
    val numOfOutputs = 2
    
    val joinOutDF = joinDataFrame1(deltaDF, masterDF, joinC1, joinType1, numOfOutputs)
    joinOutDF.foreach(_.show)*/
    
  }
  
}