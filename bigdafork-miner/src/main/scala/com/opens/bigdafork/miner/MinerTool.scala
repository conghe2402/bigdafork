package com.opens.bigdafork.miner

import java.util.Date

import com.opens.bigdafork.miner.exception.MinerException
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame

trait MinerHandleAware extends Serializable {
    def handle(sc : SparkContext, hc : HiveContext, input : MTInput, output : MTOutput)
}

abstract class MinerTool(val debug : Boolean = false) extends MinerHandleAware {
    val TOOL_NAME = this.getClass.getSimpleName
    def setParams(para : MTParams)
    def minerAction(sc : SparkContext, hc : HiveContext, input : MTInput, output : MTOutput) : Unit

    def loadSrc(sc : SparkContext, hc : HiveContext, input : MTInput) : DataFrame = {
        if (input.srcType == 0) {
            hc.sql(s"select * from ${input.src}")
        } else if (input.srcType == 1) {
            hc.read.format(input.fileFormat)
              .option("delimiter", input.delimiter).load(input.src)
        } else {
            throw new MinerException(Seq(s"${input.srcType} is not supported"))
        }
    }

    override def handle(sc: SparkContext, hc: HiveContext, input: MTInput, output: MTOutput): Unit = {
        println(s"${TOOL_NAME} handle begin......")
        val startTime = new Date().getTime
        try {
            this.minerAction(sc, hc, input, output)
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
  */
case class MTInput(src : String, srcType : Int,
                   srcCondition : String = "",
                   fileFormat : String = "csv",
                   fieldSchema : Map[String, String] = Map.empty,
                   delimiter : String = ",") {

}

/**
  *
  * @param dest
  * @param destType 0 : table, 1 : hdfs file
  */
case class MTOutput(dest : String, destType: Int) {

}
