package com.aero.core

import org.apache.log4j.{ Level, Logger, LogManager }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.expressions.UserDefinedFunction

import com.aero.core.EtlFunc._
import com.aero.utils.implicits._
import com.aero.utils.Utils

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import net.liftweb.json.parse
import com.aero.logging.Logging
import org.rogach.scallop._

/*
               (        )
   (           )\ )  ( /(
   )\     (   (()/(  )\())
((((_)(   )\   /(_))((_)\
 )\ _ )\ ((_) (_))    ((_)
 (_)_\(_)| __|| _ \  / _ \
  / _ \  | _| |   / | (_) |
 /_/ \_\ |___||_|_\  \___/

*/

object aeroDriver extends Logging with App {

  LogManager.getRootLogger.setLevel(Level.INFO)
  logInfo("___Inside Aero Engine___")
  logInfo("parsing input params ...")
  val conf = new Conf(args)
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("etl")
    .config("spark.driver.allowMultipleContexts", "true")
    .config("spark.ui.showConsoleProgress", false)
    .config("spark.ui.port", "44040")
    .getOrCreate()
  val paramMap = if (!conf.param.supplied) {
    logInfo("no config file passed")
    Map.empty[String, String]
  } else {
    logInfo(s"parsing param file ${conf.param()}")
    parse(spark.read.text(conf.param()).rdd.map(_.getString(0)).collect().mkString).json.values.asInstanceOf[Map[String, String]]
  }
  logInfo(s"___PARAM CONFIGURATION___ ${paramMap.toString}")

  //spark.sparkContext.setLogLevel("ERROR")

  lazy val resultMap = scala.collection.mutable.Map.empty[String, DataFrame]
  lazy val udfMap = scala.collection.mutable.Map.empty[String, UserDefinedFunction]

  

  var OtherOptionsFlagEnable = "N"
  implicit val formats = DefaultFormats
  case class configs(funcLableName: String, input: String, func: String, output: String, config: String, otherOptions: String)

  try {
    etlMain
    spark.stop()
    System.exit(0)
  } catch {
    case e: Exception => {
      logError("Program failed , please check the above logs")
      spark.stop()
      System.exit(-1)
    }
  }

  def etlMain() = {

    logInfo(s"___JOB HAS BEEN STARTED___")

    val myJson = parse(spark.read.text(conf.config()).rdd.map(_.getString(0)).collect().mkString)
    val jobFlow = myJson.json.children.map(_.values.asInstanceOf[Map[String, Map[String, Any]]])
    logInfo(s"___JOB CONFIGURATION___ ")
    show(spark.createDataFrame(spark.sparkContext.parallelize(jobFlow.map(
      x => {
        val y = x.toList(0)
        val funcLableName = y._1
        val funcConf = y._2
        configs(funcLableName, funcConf.getOrElse("input", "").toString,
          funcConf.getOrElse("func", "com.aero.components.NoFuncDefined").toString,
          funcConf.getOrElse("output", "").toString,
          funcConf.getOrElse("config", "").toString,
          funcConf.getOrElse("otherOptions", "").toString)
      }))), false, "Job configuration")

    logInfo(s"___RUNNING INDIVIDUAL COMPONENTS___")
    jobFlow.foreach { x => val y = x.toList(0); executeETLfunc2(y._1, y._2) }

    logInfo(s"___JOB HAS BEEN COMPLETED___")

  }

  def executeETLfunc2(funcLableName: String, funcConf: Map[String, Any]): Unit = {

    logInfo("___COMPONENET NAME___: " + funcLableName)

    (
      funcConf.getOrElse("input", "input"),
      funcConf.getOrElse("func", "com.aero.components.NoFuncDefined").toString,
      funcConf.getOrElse("output", "output"),
      funcConf.getOrElse("config", "config"),
      if (OtherOptionsFlagEnable == "Y") funcConf.getOrElse("otherOptions", Map.empty[String, Any]) else Map.empty[String, Any]) match {

        case x if (x._2 == "SETV") => {
          //SETV
          {
            logInfo(s"_____SETV_____: ${funcLableName}")
            //val jobConfigOptions = x._4.asInstanceOf[Map[String, String]]
            OtherOptionsFlagEnable = x._4.getOrElse("otherOptionsEnable", "N")
          }
        }
        case x if (x._2 == "REGUDF") => {
          //SETV
          {
            logInfo(s"_____REGUDF_____: ${funcLableName}")
            val config = x._4.asInstanceOf[List[String]]
            REGUDF(config, spark).foreach(udfMap+=)

          }
        }
        case (in, others, out, option, otherOpt) => {
          try {
            addResult(callComp(
              others,
              resultMap.toMap,
              in,
              out,
              option,
              otherOpt).asInstanceOf[Map[String, DataFrame]])
          } catch {
            case e: scala.ScalaReflectionException if (e.getMessage.contains("not found")) =>
              logError(s"check the class name $others or if it's user supplied, make sure to add the respective jar it in classpath", e); throw e
            case e: java.lang.IllegalArgumentException =>
              logError("User defined component should extend UDCComponent/GenericComponent class and implement execute method", e); throw e
            case e: java.util.NoSuchElementException =>
              logError(e.getMessage, e); throw e
            case e: java.lang.reflect.InvocationTargetException =>
              logError("test  " + e.getMessage, e); throw e
            case e: Exception => logError(e.getClass.toString, e); throw e
          }

        }

      }

  }
  def executeETLfunc(funcLableName: String, funcConf: Map[String, Any]): Unit = {

    (
      funcConf.getOrElse("input", "input"),
      funcConf.getOrElse("func", "func").toString.toLowerCase,
      funcConf.getOrElse("output", "output"),
      funcConf.getOrElse("config", "config"),
      if (OtherOptionsFlagEnable == "Y") funcConf.getOrElse("otherOptions", Map.empty[String, Any]) else Map.empty[String, Any]) match {
        //READ
        case (in, "read", out, option, otherOpt) => {

          addResult(callComp(
            "com.aero.components.ReadDF",
            resultMap.toMap,
            in,
            out,
            option,
            otherOpt).asInstanceOf[Map[String, DataFrame]])
          /*logInfo(s"_____READ_____: ${funcLableName}")
            val inputFileConfig =
              for ((k, v) <- in.asInstanceOf[Map[String, String]]) yield {
                (k, resolveParamteres(v))
              }
            val df = READ(spark, inputFileConfig)
            logInfo("_____output_____:" + out.toString())
            OTHER_OPTIONS(df, otherOpt, out.toString)
            resultMap += (out.toString() -> df)*/
        }
        //WRITE
        case (in, "write", out, _, _) => {
          logInfo(s"_____WRITE_____: ${funcLableName}")
          val outputFileConfig =
            for ((k, v) <- out.asInstanceOf[Map[String, String]]) yield {
              (k, resolveParamteres(v))
            }
          //output
          WRITE(resultMap(in.toString), outputFileConfig)
        }
        //FILTER
        case (in, "filter", out, option, otherOpt) => {

          logInfo(s"_____FILTER_____: ${funcLableName}")
          val filterConfig = option.asInstanceOf[Map[String, Any]]
          val df = FILTER(resultMap(in.toString), filterConfig)
          val filterOut = out.asInstanceOf[Map[String, String]]
          //assign filter output. Output can contain multiple outputs
          List(filterOut("select"), filterOut.getOrElse("deSelect", "_").toString())
            .zip(df.productIterator.toList).foreach(x => resultMap += (x._1 -> x._2.asInstanceOf[DataFrame]))
          //storage level
          if (option.getOrElse("storageLevel", "N") != "N") {
            val storageConfig = option.getOrElse("storageLevel", Map.empty[String, String]).asInstanceOf[Map[String, String]]
            STORAGE(storageConfig.map(x => (resultMap(x._1), x._2)))
          }
          //other options
          val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]
          if (!otherOptKV.isEmpty) {
            for ((k, v) <- otherOptKV) {
              logInfo(s"_____output_____:$k")
              OTHER_OPTIONS(resultMap(k), v, k)
            }
          }
        }
        //DIFF
        case (in, "diff", out, option, otherOpt) => {
          logInfo(s"_____DIFF_____: ${funcLableName}")
          val df = DIFF(resultMap(in("1")), resultMap(in("2")))
          val diffOut = out.asInstanceOf[Map[String, String]]
          //assign output
          List(diffOut("1"), diffOut("2"), diffOut("3"))
            .zip(df.productIterator.toList).foreach(x => resultMap += (x._1 -> x._2.asInstanceOf[DataFrame]))
          //other options
          val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]
          if (!otherOptKV.isEmpty) {
            for ((k, v) <- otherOptKV) {
              logInfo(s"_____output_____:$k")
              OTHER_OPTIONS(resultMap(k), v, k)
            }
          }
        }
        //MINUS
        case (in, "minus", out, option, otherOpt) => {
          logInfo(s"_____MINUS_____: ${funcLableName}")
          val df = MINUS(resultMap(in("1")), resultMap(in("2")))

          //other options
          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
          resultMap += (out.toString() -> df)
        }
        //INTERSECT
        case (in, "intersect", out, option, otherOpt) => {
          logInfo(s"_____INTERSECT_____: ${funcLableName}")
          val df = INTERSECT(resultMap(in("1")), resultMap(in("2")))

          //other options
          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
          resultMap += (out.toString() -> df)
        }
        //UNION
        case (in, "union", out, option, otherOpt) => {
          logInfo(s"_____UNION_____: ${funcLableName}")
          val inputKV = in.asInstanceOf[List[String]]
          val df = UNION(inputKV.map(resultMap))

          //other options
          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
          resultMap += (out.toString() -> df)
        }
        //JOIN
        case (in, "join", out, option, otherOpt) => {
          logInfo(s"_____JOIN_____: ${funcLableName}")
          try {

            val reformatList =
              if (option.getOrElse("reformat", "N") == "N")
                Nil
              else if (option("reformat").getOrElse("filePath", "N") == "N")
                option("reformat").asInstanceOf[List[List[String]]].map(x => (x(0), x(1)))
              else {
                val JoinFileConfig = option.asInstanceOf[Map[String, Map[String, String]]]
                val filePath = JoinFileConfig("reformat")("filePath").asInstanceOf[String]
                val fileName = JoinFileConfig("reformat")("fileName").asInstanceOf[String]
                scala.io.Source.fromFile(filePath + fileName)
                  .getLines().toList
                  .map(x => (x.split("=>")(0).trim, x.split("=>")(1).trim))
              }

            val joinOutList =
              List(
                out.getOrElse("out", "_").toString,
                out.getOrElse("new", "_").toString,
                out.getOrElse("old", "_").toString,
                out.getOrElse("leftMatch", "_").toString,
                out.getOrElse("rightMatch", "_").toString)

            val rightJoinKeys =
              if (option.getOrElse("rightKeys", "N") != "N")
                option("rightKeys").asInstanceOf[List[String]]
              else
                Nil

            val df = JOINE(
              spark,
              resultMap(in("left")),
              resultMap(in("right")),
              option("leftKeys").asInstanceOf[List[String]],
              rightJoinKeys,
              option.getOrElse("leftSelectCond", "").asInstanceOf[String],
              option.getOrElse("rightSelectCond", "").asInstanceOf[String],
              reformatList,
              option("joinType").asInstanceOf[String],
              joinOutList)

            joinOutList.zip(df.productIterator.toList).foreach(x => resultMap += (x._1 -> x._2.asInstanceOf[DataFrame]))

            val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]

            if (!otherOptKV.isEmpty) {
              for ((k, v) <- otherOptKV) {
                logInfo(s"_____output_____:$k")
                OTHER_OPTIONS(resultMap(k), v, k)
              }
            }
          } catch {
            case ex: NoSuchElementException =>
              log.error(s"Error in JOIN component: '$funcLableName', Error =>  ${ex.getMessage}", ex); throw ex
            case ex: RuntimeException =>
              log.error(s"Error in JOIN component: '$funcLableName', Error =>  ${ex.getMessage}", ex); throw ex
          }
        }
        //GROUPBY
        case (in, "groupbye", out, option, otherOpt) => {
          logInfo(s"_____GROUPBY_____: ${funcLableName}")
          val groupbyConfig = option.asInstanceOf[Map[String, Any]]
          //optional fields
          val groupKey =
            if (groupbyConfig.getOrElse("groupbyKeys", List.empty[String]) != null)
              groupbyConfig.getOrElse("groupbyKeys", List.empty[String]).asInstanceOf[List[String]].map(resolveParamteres(_))
            else List(null)
          //call groupby
          val df = GROUPBYE(
            resultMap(in.toString),
            groupKey,
            groupbyConfig("aggregation").asInstanceOf[List[List[String]]].map(x => (resolveParamteres(x(0)), resolveParamteres(x(1)), resolveParamteres(x(2)))))
          resultMap += (out.toString() -> df)

          //Storage level
          val storageLevel = resolveParamteres(option.getOrElse("storageLevel", "N"))
          if (storageLevel != "N") STORAGE(Map(df -> storageLevel))
          //otherOptions
          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
        }
        //SORT
        case (in, "sort", out, option, otherOpt) => {
          try {
            logInfo(s"_____SORT_____: ${funcLableName}")
            val df =
              SORT(
                resultMap(in.toString),
                option("sortKeys").asInstanceOf[List[List[String]]].map(x => (resolveParamteres(x(0)), resolveParamteres(x(1)))))
            resultMap += (out.toString() -> df)
            //Storage level
            val storageLevel = resolveParamteres(option.getOrElse("storageLevel", "N"))
            if (storageLevel != "N") STORAGE(Map(df -> storageLevel))
            //otherOptions
            logInfo("_____output_____:" + out.toString())
            OTHER_OPTIONS(df, otherOpt, out.toString)
          } catch {
            case ex: NoSuchElementException =>
              log.error(s"Error in SORT component: '$funcLableName', Error =>  ${ex.getMessage}", ex); throw ex
            case ex: RuntimeException =>
              log.error("Error in SORT component '" + funcLableName + "', Error => " + ex.getMessage, ex); throw ex
          }
        }
        //DEDUP
        case (in, "dedup", out, option, otherOpt) => {
          logInfo(s"_____DEDUP_____: ${funcLableName}")

          val dedupConfig = option.asInstanceOf[Map[String, Any]]
          val df = DEDUP(
            resultMap(in.toString),
            dedupConfig("keys").asInstanceOf[List[String]],
            (dedupConfig -- List("keys")))

          val dedupOut = out.asInstanceOf[Map[String, String]]
          List(out("unique"), out.getOrElse("duplicate", "_").toString())
            .zip(df.productIterator.toList).foreach(x => resultMap += (x._1 -> x._2.asInstanceOf[DataFrame]))

          val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]

          if (!otherOptKV.isEmpty) {
            for ((k, v) <- otherOptKV) {
              logInfo(s"_____output_____:$k")
              OTHER_OPTIONS(resultMap(k), v, k)
            }
          }
        }
        //DROPDUP
        case (in, "dropdup", out, option, otherOpt) => {
          logInfo(s"_____DROPDUP_____: ${funcLableName}")
          val df = DROP_DUPLICATES(resultMap(in.toString), option("keys").asInstanceOf[List[String]], option("keep").asInstanceOf[String])

          //otherOptions
          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
          resultMap += (out.toString() -> df)
        }
        //SELECT
        case (in, "select", out, option, otherOpt) => {
          logInfo(s"_____SELECT_____: ${funcLableName}")
          val df = SELECT(resultMap(in.toString), option("select").asInstanceOf[List[String]])

          //otherOptions
          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
          resultMap += (out.toString() -> df)
        }
        //DROP
        case (in, "drop", out, option, otherOpt) => {
          logInfo(s"_____DROP_____: ${funcLableName}")
          val df = DROP(resultMap(in.toString), option("drop").asInstanceOf[List[String]])

          //otherOptions
          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
          resultMap += (out.toString() -> df)
        }
        //REFORMAT
        case (in, "reformat", out, option, otherOpt) => {
          logInfo(s"_____REFORMAT_____: ${funcLableName}")
          val reformatConfig = option.asInstanceOf[Map[String, Map[String, String]]]

          val reformatExp =
            option.keySet.toSeq match {
              case key if key.contains("reformatCol") && key.contains("reformatFile") => {
                val errorMsg =
                  s"invalid reformat - cannot be provided both 'reformatCol' and 'reformatFile' in ' ${funcLableName} ' \n ' ___ ${funcConf} ___ '"
                log.error(errorMsg)
                System.exit(-1)
                List.empty[(String, String)]
              }
              case key if key.contains("reformatCol") => reformatConfig("reformatCol").asInstanceOf[List[List[String]]].map(x => (x(0), x(1)))
              case key if key.contains("reformatFile") => {
                val filePath = reformatConfig("reformatFile")("filePath")
                val fileName = reformatConfig("reformatFile")("fileName")
                scala.io.Source.fromFile(filePath + fileName)
                  .getLines().toList
                  .filter(!_.startsWith("#"))
                  .map(x => (x.split("=>")(0).trim, x.split("=>")(1).trim))
              }
              case _ => {
                val errorMsg =
                  s"invalid reformat - either 'reformatCol' or 'reformatFile' needs to be provided in ' ${funcLableName} ' \n '___ ${funcConf} ___'"
                log.error(errorMsg)
                System.exit(-1)
                List.empty[(String, String)]
              }
            }

          val df = REFORMAT(resultMap(in.toString), reformatExp)
          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)

          resultMap += (out.toString() -> df)
        }

        /*       case (in, "mreformat", out, option, otherOpt) => {
          val df = MREFORMAT(resultMap(in.toString),
              option("mReformat").asInstanceOf[Map[String, List[(String, String)]]],
              option("mReformatCond").asInstanceOf[Map[String, String]])
          df.map(entry => resultMap += (entry._1 -> entry._2))
        }*/

        //LIMIT
        case (in, "limit", out, option, otherOpt) => {
          logInfo(s"_____LIMIT_____: ${funcLableName}")
          val limitConfig = option.asInstanceOf[Map[String, String]]
          val df = LIMIT(resultMap(in.toString), option("limit"))

          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)

          resultMap += (out.toString() -> df)
        }
        //SEQUENCE
        case (in, "sequence", out, option, otherOpt) => {
          logInfo(s"_____SEQUENCE_____: ${funcLableName}")
          val sequnceConfig = option.asInstanceOf[Map[String, String]]
          val seqStartVal =
            if (sequnceConfig("startValue").isInstanceOf[String]) {
              sequnceConfig("startValue").asInstanceOf[String].toInt
            } else {
              val seqFromConfig = sequnceConfig("startValue").getOrElse("valueFrom", Map.empty[String, List[String]]).asInstanceOf[Map[String, List[String]]]
              val seqFromConfigList = seqFromConfig.toList(0)
              COLUMN_FUNC(resultMap(seqFromConfigList._1), Map(seqFromConfigList._2(1) -> seqFromConfigList._2(0))).toInt
            }

          val df = SEQUENCE(spark, resultMap(in.toString),
            seqStartVal,
            sequnceConfig("stepValue").asInstanceOf[String].toInt,
            sequnceConfig("targetColumn").asInstanceOf[String])

          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)

          resultMap += (out.toString() -> df)
        }

        //SEQUENCE WITH SORT
        case (in, "sequence_with_sort", out, option, otherOpt) => {
          logInfo(s"_____SEQUENCE_____: ${funcLableName}")

          val df = SEQUENCE_WITH_SORT(
            resultMap(in.toString),
            option("startValue").toInt,
            option("stepValue").toInt,
            option.getOrElse("targetColumn", "-"),
            option.getOrElse("sortColumns", Nil).asInstanceOf[List[String]],
            option.getOrElse("partitionColumns", Nil).asInstanceOf[List[String]])

          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)

          resultMap += (out.toString() -> df)
        }
        //SQL
        case (in, "sql", out, option, otherOpt) => {
          logInfo(s"_____SQL_____: ${funcLableName}")

          val sqlConfig = option.asInstanceOf[Map[String, String]]

          val df = SQL(
            spark,
            in.map(entry => (entry._2 -> resultMap(entry._2))),
            option("sql"))

          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)

          resultMap += (out.toString() -> df)
        }
        //LOOKUP
        case (in, "lookup", out, option, otherOpt) => {
          logInfo(s"_____LOOKUP_____: ${funcLableName}")

          val df = LOOKUPE(
            resultMap(in("input")),
            resultMap(in("lookup")),
            option("keys").asInstanceOf[List[String]],
            option.getOrElse("lookupKeys", Nil).asInstanceOf[List[String]],
            option("lookupColumn"))

          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)

          resultMap += (out.toString() -> df)
        }
        //EXIT
        case (in, "exit", _, option, _) => {
          logInfo(s"_____EXIT_____: ${funcLableName}")
          val df = EXIT(
            resultMap(in.toString),
            option)
        }
        //STORAGE
        case (_, "storage", _, option, otherOpt) => {
          logInfo(s"_____STORAGE_____: ${funcLableName}")
          STORAGE(option.map(entry => (resultMap(entry._1) -> entry._2)))
          val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]

          if (!otherOptKV.isEmpty) {
            for ((k, v) <- otherOptKV) {
              logInfo(s"_____output_____:$k")
              OTHER_OPTIONS(resultMap(k), v, k)
            }
          }
        }
        //PARTITION
        case (in, "partition", out, option, otherOpt) => {
          logInfo(s"_____PARTITION_____: ${funcLableName}")
          val partitionConfig = option.asInstanceOf[Map[String, Any]]
          val numOfPartitions =
            if (partitionConfig.getOrElse("numberOfPartitions", 0) == 0)
              0
            else {
              if (partitionConfig("numberOfPartitions").isInstanceOf[String])
                partitionConfig("numberOfPartitions").asInstanceOf[String].toInt
              else
                partitionConfig("numberOfPartitions").asInstanceOf[BigInt].toInt
            }

          //logInfo(partitionConfig)
          val df = PARTITION(
            resultMap(in.toString),
            partitionConfig("partitionByColumns").asInstanceOf[List[String]],
            numOfPartitions)
          resultMap += (out.toString() -> df)

          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
        }
        //SORT_PARTITION
        case (in, "sort_partition", out, option, otherOpt) => {
          logInfo(s"_____SORT_PARTITION_____: ${funcLableName}")
          val df =
            SORT_PARTITION(
              resultMap(in.toString),
              option("sortKeys").asInstanceOf[List[String]])
          resultMap += (out.toString() -> df)

          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
        }
        //NORMALIZE
        case (in, "normalize", out, option, otherOpt) => {
          logInfo(s"_____NORMALIZE_____: ${funcLableName}")

          val normalizeSChema =
            if (option.getOrElse("schema", "N") != "N") {
              val schemaJson = write(option("schema"))
              Map("schema" -> schemaJson.mkString)
            } else Map.empty[String, String]
          //logInfo(normalizeSChema)
          val normalizeKey = Map("normalizeKey" -> option("normalizeKey"))

          val df = NORMALIZE(
            resultMap(in.toString),
            normalizeKey ++ normalizeSChema,
            option("select").asInstanceOf[List[String]])
          resultMap += (out.toString() -> df)

          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
        }

        case (in, "pivot", out, option, otherOpt) => {
          logInfo(s"_____PIVOT_____: ${funcLableName}")

          val df =
            PIVOT(
              resultMap(in.toString),
              option("groupbyKeys").asInstanceOf[List[String]],
              option("pivotColumn"),
              option("aggregation").asInstanceOf[List[List[String]]].map(x => (x(0), x(1))))

          resultMap += (out.toString() -> df)

          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
        }

        case (in, "cumulative", out, option, otherOpt) => {

          logInfo(s"_____CUMULATIVE_____: ${funcLableName}")

          val df =
            CUMULATIVE(
              resultMap(in.toString),
              option("keys").asInstanceOf[List[String]],
              option("orderBy").asInstanceOf[List[String]],
              (option("cumulativeFunction"), option("cumulativeColumn"), option("targetColumn")),
              'N')

          resultMap += (out.toString() -> df)

          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
        }

        case (in, "scd1", out, option, otherOpt) => {

          logInfo(s"_____SCD1_____: ${funcLableName}")

          val df =
            SCD1(
              spark,
              resultMap(in("left")),
              resultMap(in("right")),
              option("joinKeys").asInstanceOf[List[String]],
              (option("startDateColumn"), resolveParamteres(option("startDate"))))

          resultMap += (out.toString() -> df)
          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
        }

        case (in, "scd2", out, option, otherOpt) => {

          logInfo(s"_____SCD2_____: ${funcLableName}")

          val df =
            SCD2(
              spark,
              resultMap(in("left")),
              resultMap(in("right")),
              option("joinKeys").asInstanceOf[List[String]],
              (option("startDateColumn"), resolveParamteres(option("startDate"))), (option("endDateColumn"), resolveParamteres(option("endDate"))))

          resultMap += (out.toString() -> df)

          logInfo("_____output_____:" + out.toString())
          OTHER_OPTIONS(df, otherOpt, out.toString)
        }

        /*

        case (in, "scan", out, option) => {
          val df = SCAN (resultMap(in.toString),
            option("keyCol").asInstanceOf[List[String]],
            option("orderCol").asInstanceOf[List[String]],
            (option("FuncColCond")(0),option("FuncColCond")(1),option("FuncColCond")(2),option("FuncColCond")(3)),
            option("orderCol").asInstanceOf[Char])
            resultMap += (out.toString() -> df)
        }

        */
        case x if (x._2 == "setv") => {
          //SETV
          {
            logInfo(s"_____SETV_____: ${funcLableName}")
            //val jobConfigOptions = x._4.asInstanceOf[Map[String, String]]
            OtherOptionsFlagEnable = x._4.getOrElse("otherOptionsEnable", "N")
          }
        }
        case x if (x._2 == "regudf") => {
          //SETV
          {
            logInfo(s"_____REGUDF_____: ${funcLableName}")
            val config = x._4.asInstanceOf[List[String]]
            REGUDF(config, spark).foreach(udfMap+=)

          }
        }
        case (in, "custcomp", out, option, otherOpt) => {
          //SETV

          addResult(callComp(
            option.toString(),
            resultMap.filter(x => in.asInstanceOf[List[String]].contains(x._1)).toMap,
            in,
            out,
            option,
            otherOpt).asInstanceOf[Map[String, DataFrame]])

          /*logInfo(s"_____CUSTOMCOMP_____: ${funcLableName}")
            val outDF = CUSTCOMP(
              resultMap.filter(x => in.asInstanceOf[List[String]].contains(x._1)).toMap,
              option.toString(),
              out.asInstanceOf[List[String]],
              spark)
            outDF.foreach(resultMap += _)
            outDF.foreach(x => OTHER_OPTIONS(x._2, otherOpt(x._1)))*/
        }
        case _ => {
          log.error(s"invalid statemenet:  -  ' ${funcLableName} ' \n '___ ${funcConf} ___'")
          System.exit(-1)
        }

      }

    /*case class Caller[T >: Null <: Any](klass: T) {

    }
    implicit def anyref2callable[T >: Null <: AnyRef](klass: T): Caller[T] = new Caller(klass)
*/
  }
  def resolveParamteres(parmString: String): String = {
    val parmPattern = "\\$\\{(\\w+)\\}".r
    if (parmString != null) {
      parmPattern.findFirstIn(parmString) match {
        case None => parmString
        case x => {
          val parmPatternField = "\\w+".r
          val pField = parmPatternField.findFirstIn(x.get)
          val rstr = parmString.replace(x.get, paramMap(pField.get))
          resolveParamteres(rstr)
        }
      }
    } else parmString
  }

  def callComp(componenetName: String, args: Any*): Any = {
    import scala.reflect.runtime.{ universe => ru }
    val m = ru.runtimeMirror(getClass.getClassLoader)
    import ru._
    val module = m.staticModule(componenetName)
    val im = m.reflectModule(module)
    val execute = im.symbol.info.member(ru.TermName("execute")).asMethod
    val objMirror = m.reflect(im.instance)
    objMirror.reflectMethod(execute)(args: _*)
  }

  def addResult(df: Map[String, DataFrame]) = df.foreach(resultMap+=)

  def otherOptionsForDF(DFMap: Any, otherOpt: Map[String, Any], noOfDFs: Char) = {
    noOfDFs match {
      case 'S' => {
        val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]
        if (!otherOptKV.isEmpty) {
          val outDFKey = DFMap.toList.map(x => x._1)
          val outDF = DFMap.toList.map(x => x._2)
          logInfo("_____output_____:" + outDFKey.mkString)
          OTHER_OPTIONS(outDF.mkString.asInstanceOf[DataFrame], otherOptKV, outDFKey.mkString)
        }
      }
      case 'M' => {
        val otherOptKV = otherOpt.asInstanceOf[Map[String, Map[String, String]]]
        val otherDFMap = DFMap.asInstanceOf[Map[String, DataFrame]]
        if (!otherOptKV.isEmpty) {
          for ((k, v) <- otherOptKV) {
            logInfo(s"_____output_____:$k")
            OTHER_OPTIONS(otherDFMap(k), v.asInstanceOf[Map[String, String]],k)
          }
        }
      }
    }

  }

}
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) with Serializable {
  val config = opt[String](required = true)
  val param = opt[String](required = true, default = Option(""))
  verify()
}