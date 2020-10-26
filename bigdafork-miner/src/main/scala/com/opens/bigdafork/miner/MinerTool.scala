package com.opens.bigdafork.miner

import java.util.Date

import com.opens.bigdafork.miner.ability.{TDataFrameRowAware, TLibSVMAware}
import com.opens.bigdafork.miner.exception.MinerException
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable.LinkedHashMap

trait MinerHandleAware extends Serializable {
    def handle(sc : SparkContext, hc : HiveContext, input : MTInput, output : MTOutput)
}

abstract class MinerTool(val debug : Boolean = false) extends MinerHandleAware
                                                         with TLibSVMAware with TDataFrameRowAware {
    val TOOL_NAME = this.getClass.getSimpleName
    protected val sampleSize : Int = 20

    def setParams(para : MTParams) : Unit = {}

    def minerAction(sc : SparkContext, hc : HiveContext, input : DataFrame) : DataFrame

    protected def printSampleData(df : DataFrame) : Unit = {
        df.take(sampleSize).foreach(x => {println(x.mkString(","))})
    }

    def loadSrc(sc : SparkContext, hc : HiveContext, input : MTInput) : DataFrame = {
        if (input.srcType == 0) {
            val fields = input.fieldSchema.map(f => f._1).mkString(",")
            val fieldsExpr = if (fields == "") "*" else fields
            hc.sql(s"select ${fieldsExpr} from ${input.src}")
        } else if (input.srcType == 1) {
            hc.read.format(input.fileFormat)
              .option("delimiter", input.delimiter).load(input.src)
        } else {
            throw new MinerException(Seq(s"${input.srcType} is not supported"))
        }
    }

    def writeRes(sc : SparkContext, hc : HiveContext, resultDF : DataFrame, output : MTOutput) : Unit = {
        if (resultDF == null) return

        println("output result...")
        if (output.destType == 0) {
            resultDF.registerTempTable(s"temp_${output.dest}")
            hc.sql(s"drop table if exists ${output.dest}")
            hc.sql(s"create table if not exists ${output.dest} as select * from temp_${output.dest} limit 1")
            hc.sql(s"truncate table ${output.dest}")
            resultDF.write.mode(SaveMode.Overwrite).insertInto(output.dest)
        }

    }

    override def handle(sc: SparkContext, hc: HiveContext, input: MTInput, output: MTOutput): Unit = {
        println(s"${TOOL_NAME} handle begin......")
        val startTime = new Date().getTime

        try {
            val inputDF = loadSrc(sc, hc, input)
            if (debug) {
                println("original sample data: ")
                printSampleData(inputDF)
            }
            val resultDF = this.minerAction(sc, hc, inputDF)
            if (debug) {
                println("result df sample data: ")
                printSampleData(resultDF)
            }
            writeRes(sc, hc, resultDF, output)
        } catch {
            case ex : MinerException => println(ex.getMessage)
            case ex : Exception => {
                ex.printStackTrace()
                println(ex.getMessage)
            }
        }
        val costTime = ((new Date().getTime - startTime) % 3600000) / 60000
        println(f"${TOOL_NAME} handle complete after ${costTime} minutes")
    }

    protected def addApos(colName : String): String = s"`${colName}`"
}

class MTParams {

}

/**
  *
  * @param src
  * @param srcType 0 : table, 1 : hdfs file
  * @param srcCondition
  * @param fileFormat
  * @param delimiter
  */
case class MTInput(src : String, srcType : Int = 0,
                   srcCondition : String = "",
                   fileFormat : String = "csv",
                   delimiter : String = ",") {

    assert(src != null && src.trim != "", "src can not be set to empty")

    var fieldSchema : LinkedHashMap[String, String] = LinkedHashMap.empty

    def setJFieldSchema(fields : java.util.LinkedHashMap[String, String]) : Unit = {
        val it = fields.keySet().iterator()
        while (it.hasNext) {
            val key = it.next()
            fieldSchema.+=((key, fields.get(key)))
        }
    }

    def setFieldSchema(fields : LinkedHashMap[String, String] ) : Unit = {
        this.fieldSchema = fields
    }

}

/**
  *
  * @param dest
  * @param destType 0 : table, 1 : hdfs file
  */
case class MTOutput(dest : String, destType: Int = 0) {
    assert(dest != null && dest.trim != "", "dest can not be set to empty")
}
