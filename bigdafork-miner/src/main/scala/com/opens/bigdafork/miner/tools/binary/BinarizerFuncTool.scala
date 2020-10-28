package com.opens.bigdafork.miner.tools.binary

import com.opens.bigdafork.miner.{MTParams, MinerTool}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import java.util.{LinkedHashMap => JMap}
import com.opens.bigdafork.miner.util.CollectionUtils
import org.apache.spark.sql.functions.col
import scala.collection.mutable.LinkedHashMap

/**
  * Binarizer tool. Binarizer only double type field.
  * @param debug
  */
class BinarizerFuncTool (implicit debug : Boolean = false) extends MinerTool(debug) {

    private var params: BinarizerFuncParams = null

    override def setParams(para: MTParams) : Unit = {
        this.params = para.asInstanceOf[BinarizerFuncParams]
    }

    override def minerAction(sc: SparkContext, hc: HiveContext, inputDF: DataFrame): DataFrame = {
        var dataFrame = inputDF
        val binarizer = new Binarizer().setThreshold(params.threshold)
        for (f <- params.binFields) {
            binarizer.setInputCol(f._2).setOutputCol(s"${f._2}_${f._1}")
            dataFrame = binarizer.transform(dataFrame)
                                 .withColumn(f._2, col(s"${f._2}_${f._1}"))
                                 .drop(col(s"${f._2}_${f._1}"))
        }

        dataFrame
    }
}


/**
  * params.
  */
class BinarizerFuncParams(var threshold : Double = 0.5) extends MTParams {
    var binFields : LinkedHashMap[Integer, String] = LinkedHashMap.empty
    def setBinFields(binFieldsParam : LinkedHashMap[Int, String]) : Unit = {
        binFields = binFieldsParam.map(x => (Integer.valueOf(x._1), x._2))
    }

    def setJBinFields(binFieldsParam : JMap[Integer, String]) : Unit = {
        CollectionUtils.asScalaLinkedHashMap[JMap[Integer, String], LinkedHashMap[Integer, String]](binFieldsParam, binFields)
    }

    def setThreshold(threshold : Double) : Unit = {
        this.threshold = threshold
    }
}