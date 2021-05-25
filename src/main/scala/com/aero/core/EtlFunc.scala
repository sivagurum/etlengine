package com.aero.core

import org.apache.spark.sql.{ Encoder, Dataset, DataFrame, Row }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.expressions.UserDefinedFunction
import com.aero.logging.Logging
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import java.sql.{ Date, Timestamp }
import org.apache.commons.lang3.StringUtils
import scala.util.control.NonFatal
import org.apache.log4j.LogManager
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.{ QueryExecution, SQLExecution }
import com.aero.utils.implicits._

object EtlFunc extends Logging {

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
  val EXIT = com.aero.components.ExitDF.exitDataFrame _
  val MINUS = com.aero.components.MinusDF.minusDataFrame _
  val INTERSECT = com.aero.components.IntersectDF.intersectDataFrame _
  val MFILTER = com.aero.components.MultiFilterDF.multiFilterDataFrame _
  val LOOKUPE = com.aero.components.LookupDF.lookupDataFrameE _
  val RENAME_COLUMNS = com.aero.components.RenameColumnsDF.renameColumnsDataFrame _
  val JOINE = com.aero.components.JoinDF2.joinDataFrameE _
  val SEQUENCE_WITH_SORT = com.aero.components.SequenceSortDF.sequenceDataFrameWindow _
  val COLUMN_FUNC = com.aero.components.ColumnFunctionsDF.columnFunctionDataFrame _
  val GROUPBYE = com.aero.components.GroupByDF2.groupByDataFrameE _
  val SELECT = com.aero.components.SelectDF.selectDataFrame _
  val DROP = com.aero.components.DropDF.dropDataFrame _
  val REGUDF = registerUDF _
  val CUSTCOMP = customComp _
  val OTHER_OPTIONS = applyOtherOptionsDF (_:DataFrame,_:Map[String,String])(_:String)

  def registerUDF(udfList: List[String], spark: SparkSession): Map[String, UserDefinedFunction] = {
    import scala.reflect.runtime.{ universe => ru }
    import scala.tools.reflect.{ ToolBox => tb }
    val m = ru.runtimeMirror(getClass.getClassLoader)
    import ru._
    //val toolbox = m.mkToolBox()
    udfList.map(udf => udf -> {
      val module = m.staticModule(udf)
      val im = m.reflectModule(module)
      val init = im.symbol.info.member(ru.TermName("init")).asMethod
      val objMirror = m.reflect(im.instance)
      objMirror.reflectMethod(init)(spark).asInstanceOf[UserDefinedFunction]
    }).toMap
  }

  def customComp(in: Map[String, DataFrame], udc: String, out: List[String], spark: SparkSession): Map[String, DataFrame] = {
    import scala.reflect.runtime.{ universe => ru }
    import scala.tools.reflect.{ ToolBox => tb }
    val m = ru.runtimeMirror(getClass.getClassLoader)
    import ru._
    //val toolbox = m.mkToolBox()
    val module = m.staticModule(udc)
    val im = m.reflectModule(module)
    val init = im.symbol.info.member(ru.TermName("init")).asMethod
    val objMirror = m.reflect(im.instance)
    objMirror.reflectMethod(init)(in, out, spark).asInstanceOf[Map[String, DataFrame]]
  }
  //apply other functions on dataframe
  def applyOtherOptionsDF(inputDF: DataFrame, options: Map[String, String])(implicit name: String = "No DF name supplied") = {
    
    if (!options.isEmpty) {
      logInfo(s"___Applying Other Options on dataframe:${name}___: $options")
      if (options.getOrElse("storage", "N") == "N") () else STORAGE(Map(inputDF -> options.getOrElse("storage", "N")))

      if (options.getOrElse("show", "N") == "Y") show(inputDF, false, name)
      else if (options.getOrElse("show", "N") == "N") ()
      else show(inputDF, options.getOrElse("show", "20").toInt, false, name)

      if (options.getOrElse("printSchema", "N") == "Y") logInfo("___Printing Schema___:" + inputDF.schema.treeString)
      if (options.getOrElse("dtypes", "N") == "Y") {
        logInfo(s"___Datatype___\n${inputDF.dtypes.toString}")
      }
      if (options.getOrElse("count", "N") == "Y") {
        logInfo("___Record count___:" + inputDF.count)
      }
      if (options.getOrElse("explain", "N") == "Y") explain(inputDF)
      if (options.getOrElse("countOnColumn", "N") == "N") ()
      else {
        val countCol = options.getOrElse("countOnColumn", "").toString()
        val countExpr = "count(" + countCol + ") as " + "count_" + countCol
        show(inputDF.selectExpr(countExpr),name)
      }
      if (options.getOrElse("numOfPartitions", "N") == "Y") logInfo(s"___Number of Partitions___:" + inputDF.rdd.getNumPartitions)
      if (options.getOrElse("exit", "N") == "Y") {
        logInfo("___Exiting Job__")
        SparkSession.builder().getOrCreate().stop()
        System.exit(0)
      }
    }
  }
  

  def explain(df: DataFrame, extended: Boolean): Unit = {
    val sparkSession = SparkSession.builder.getOrCreate()
    val explain = ExplainCommand(df.queryExecution.logical, extended = extended)
    logInfo("___Explaining Query Plan___\n" + sparkSession.sessionState.executePlan(explain).executedPlan.executeCollect().mkString)
  }
  def explain(df: DataFrame): Unit = explain(df, extended = false)
  def show(df: DataFrame, name: String): Unit = show(df, 20, name)
  def show(df: DataFrame, numRows: Int, name: String): Unit = show(df, numRows, truncate = true, name)
  def show(df: DataFrame, truncate: Boolean, name: String): Unit = show(df, 20, truncate, name)
  def show(df: DataFrame, numRows: Int, truncate: Int, name: String): Unit = show(df, numRows, truncate, vertical = false, name)
  def show(df: DataFrame, numRows: Int, truncate: Boolean, name: String): Unit = show(df, numRows, if (truncate) 20 else 0, name)
  def show(df: DataFrame, numRows: Int, truncate: Int, vertical: Boolean, name: String): Unit =
    logInfo(s"___Show Dataframe___:$name\n" + showString(df, numRows, truncate, vertical))
  def showString(
    df: DataFrame,
    _numRows: Int, truncate: Int = 20, vertical: Boolean = false): String = {
    val schema = df.schema
    def toString: String = {
      try {
        val builder = new StringBuilder
        val fields = schema.take(2).map {
          case f => s"${f.name}: ${f.dataType.simpleString(2)}"
        }
        builder.append("[")
        builder.append(fields.mkString(", "))
        if (schema.length > 2) {
          if (schema.length - fields.size == 1) {
            builder.append(" ... 1 more field")
          } else {
            builder.append(" ... " + (schema.length - 2) + " more fields")
          }
        }
        builder.append("]").toString()
      } catch {
        case NonFatal(e) =>
          s"Invalid tree; ${e.getMessage}"
      }
    }

    val numRows = _numRows.max(0).min(Int.MaxValue - 1)
    val takeResult = df.take(numRows + 1)
    val hasMoreData = takeResult.length > numRows
    val data = takeResult.take(numRows)
    val sparkSession = SparkSession.builder().getOrCreate()

    lazy val timeZone =
      DateTimeUtils.getTimeZone(sparkSession.sessionState.conf.sessionLocalTimeZone)

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond `truncate` characters, replace it with the
    // first `truncate-3` and "..."
    val rows: Seq[Seq[String]] = schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] => array.mkString("[", ", ", "]")
          case seq: Seq[_] => seq.mkString("[", ", ", "]")
          case d: Date =>
            DateTimeUtils.dateToString(DateTimeUtils.fromJavaDate(d))
          case ts: Timestamp =>
            DateTimeUtils.timestampToString(DateTimeUtils.fromJavaTimestamp(ts), timeZone)
          case _ => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else {
          str
        }
      }: Seq[String]
    }

    val sb = new StringBuilder
    val numCols = schema.fieldNames.length
    // We set a minimum column width at '3'
    val minimumColWidth = 3

    if (!vertical) {
      // Initialise the width of each column to a minimum value
      val colWidths = Array.fill(numCols)(minimumColWidth)

      // Compute the width of each column
      for (row <- rows) {
        for ((cell, i) <- row.zipWithIndex) {
          colWidths(i) = math.max(colWidths(i), cell.length)
        }
      }

      // Create SeparateLine
      val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString()

      // column names
      rows.head.zipWithIndex.map {
        case (cell, i) =>
          if (truncate > 0) {
            StringUtils.leftPad(cell, colWidths(i))
          } else {
            StringUtils.rightPad(cell, colWidths(i))
          }
      }.addString(sb, "|", "|", "|\n")

      sb.append(sep)

      // data
      rows.tail.foreach {
        _.zipWithIndex.map {
          case (cell, i) =>
            if (truncate > 0) {
              StringUtils.leftPad(cell.toString, colWidths(i))
            } else {
              StringUtils.rightPad(cell.toString, colWidths(i))
            }
        }.addString(sb, "|", "|", "|\n")
      }

      sb.append(sep)
    } else {
      // Extended display mode enabled
      val fieldNames = rows.head
      val dataRows = rows.tail

      // Compute the width of field name and data columns
      val fieldNameColWidth = fieldNames.foldLeft(minimumColWidth) {
        case (curMax, fieldName) =>
          math.max(curMax, fieldName.length)
      }
      val dataColWidth = dataRows.foldLeft(minimumColWidth) {
        case (curMax, row) =>
          math.max(curMax, row.map(_.length).reduceLeftOption[Int] {
            case (cellMax, cell) =>
              math.max(cellMax, cell)
          }.getOrElse(0))
      }

      dataRows.zipWithIndex.foreach {
        case (row, i) =>
          // "+ 5" in size means a character length except for padded names and data
          val rowHeader = StringUtils.rightPad(
            s"-RECORD $i", fieldNameColWidth + dataColWidth + 5, "-")
          sb.append(rowHeader).append("\n")
          row.zipWithIndex.map {
            case (cell, j) =>
              val fieldName = StringUtils.rightPad(fieldNames(j), fieldNameColWidth)
              val data = StringUtils.rightPad(cell, dataColWidth)
              s" $fieldName | $data "
          }.addString(sb, "", "\n", "\n")
      }
    }

    // Print a footer
    if (vertical && data.isEmpty) {
      // In a vertical mode, print an empty row set explicitly
      sb.append("(0 rows)\n")
    } else if (hasMoreData) {
      // For Data that has more than "numRows" records
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }

}