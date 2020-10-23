package com.opens.bigdafork.miner.tools.scaler

import com.opens.bigdafork.miner.exception.MinerException
import com.opens.bigdafork.miner.tools.scaler.ScalerType.ScalerType
import com.opens.bigdafork.miner.{MTParams, MinerTool}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import java.util.{LinkedHashMap => JMap}

import com.opens.bigdafork.miner.util.CollectionUtils

import scala.collection.mutable.LinkedHashMap

/**
  * Three type scaler function.
  * 1.Standard
  * 2.Min-Max
  * 3.Z-index
  *
  * @param debug
  */
class ScalerTool(implicit debug : Boolean = false) extends MinerTool(debug) {

    private var params: ScalerFuncParams = null

    override def setParams(para: MTParams) : Unit = {
        this.params = para.asInstanceOf[ScalerFuncParams]
    }

    override def minerAction(sc: SparkContext, hc: HiveContext, inputDF: DataFrame): DataFrame = {
        val scalerAction = params.scalerType match {
            case ScalerType.MIN_MAX => new MinMaxScalerAction(params)
            case ScalerType.STANDARD => new StandardScalerAction(params)
            case ScalerType.Z_INDEX => new ZIndexScalerAction(params)
            case _ => throw new MinerException(Seq(s"The scaler type ${params.scalerType} is not support!"))
        }

        scalerAction.minerAction(sc, hc, inputDF)
    }
}

case class ScalerFuncParams(var scalerType: ScalerType = ScalerType.STANDARD) extends MTParams {
    var meanForStd : Boolean = false
    var varianceForStd : Boolean = true
    var noScalerField : LinkedHashMap[Integer, String] = LinkedHashMap.empty

    def setScalerType(scalerTypeParam : ScalerType) : Unit = {
        this.scalerType = scalerTypeParam
    }

    def setMeanForStd(withMean : Boolean) : Unit = {
        this.meanForStd = withMean
    }

    def setVarianceForStd(withVariance : Boolean) : Unit = {
        this.varianceForStd = withVariance
    }

    def setNonNormField(noNormFieldParam : LinkedHashMap[Int, String]) : Unit = {
        noScalerField = noNormFieldParam.map(x => (Integer.valueOf(x._1), x._2))
    }

    def setJNonNormFiled(noNormFieldParam : JMap[Integer, String]) : Unit = {
        CollectionUtils.asScalaLinkedHashMap[JMap[Integer, String], LinkedHashMap[Integer, String]](noNormFieldParam, noScalerField)
    }

}

case class StandardScalerAction(params : ScalerFuncParams,
                                override implicit val debug: Boolean = false) extends MinerTool(debug) {
    override def minerAction(sc: SparkContext, hc: HiveContext, input: DataFrame): DataFrame = {
        val libSVMRDD = libSVMHandler.transfer2LibSVM(input, params.noScalerField.toMap)
        val scaler = new StandardScaler(params.meanForStd, params.varianceForStd)
                                        .fit(libSVMRDD.map(x => x._2.features))
        libSVMHandler.transfer2DataFrame(sc, hc, libSVMRDD, params.noScalerField,
                                         input.schema, params.meanForStd)(scaler.transform)
    }
}

case class MinMaxScalerAction(params : ScalerFuncParams,
                              override implicit val debug: Boolean = false) extends MinerTool(debug) {
    override def minerAction(sc: SparkContext, hc: HiveContext, input: DataFrame): DataFrame = {
        null
    }
}

case class ZIndexScalerAction(params : ScalerFuncParams,
                              override implicit val debug: Boolean = false) extends MinerTool(debug) {
    override def minerAction(sc: SparkContext, hc: HiveContext, input: DataFrame): DataFrame = {
        null
    }
}

object ScalerType extends Enumeration {
    type ScalerType = Value
    val MIN_MAX = Value(0, "min-max")
    val Z_INDEX = Value(1, "z-index")
    val STANDARD = Value(2, "standard")
}