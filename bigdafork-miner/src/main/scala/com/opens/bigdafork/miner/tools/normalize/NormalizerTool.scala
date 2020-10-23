package com.opens.bigdafork.miner.tools.normalize

import com.opens.bigdafork.miner.{MTParams, MinerTool}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.LinkedHashMap
import java.util.{LinkedHashMap => JMap}

import com.opens.bigdafork.miner.util.CollectionUtils
import scala.collection.JavaConverters._

/**
  * Lp norm. Row-Normalize is for making every row have the same unit norm, that means
  * all of rows of sample data have the same length of vector.
  * It makes convenient ability for the text data or cluster algorithm.
  * @param debug
  */
class NormalizerTool(implicit debug : Boolean = false) extends MinerTool(debug) {

    private var params: NormFuncParams = null

    override def setParams(para: MTParams) : Unit = {
        this.params = para.asInstanceOf[NormFuncParams]
    }

    override def minerAction(sc: SparkContext, hc: HiveContext, inputDF: DataFrame): DataFrame = {
        val vectors = libSVMHandler.transfer2LibSVM(inputDF, params.noNormField.toMap)
        val normalizer = new Normalizer(params.pNorm)
        libSVMHandler.transfer2DataFrame(sc, hc, vectors,
                                        params.noNormField,inputDF.schema)(normalizer.transform)
    }
}

case class NormFuncParams(var pNorm : Double = Double.PositiveInfinity) extends MTParams {
    var noNormField : LinkedHashMap[Integer, String] = LinkedHashMap.empty
    def setPNorm(pNormParam : Double) : Unit = {
        this.pNorm = pNormParam
    }

    def setNonNormField(noNormFieldParam : LinkedHashMap[Int, String]) : Unit = {
        noNormField = noNormFieldParam.map(z => {(Integer.valueOf(z._1), z._2)})
    }

    def setJNonNormFiled(noNormFieldParam : JMap[Integer, String]) : Unit = {
        CollectionUtils.asScalaLinkedHashMap[JMap[Integer, String], LinkedHashMap[Integer, String]](noNormFieldParam, noNormField)
    }
}
