package com.opens.bigdafork.miner.tools.scaler

import com.opens.bigdafork.miner.exception.MinerException
import com.opens.bigdafork.miner.tools.scaler.ScalerType.ScalerType
import com.opens.bigdafork.miner.{MTParams, MinerTool}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import java.util.{LinkedHashMap => JMap}

import com.opens.bigdafork.miner.util.CollectionUtils
import org.apache.spark.ml.feature.MinMaxScaler

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

    protected val INPUT_FEATURES = "features"
    protected val OUTPUT_FEATURES = "scaledFeatures"

    override def setParams(para: MTParams) : Unit = {
        this.params = para.asInstanceOf[ScalerFuncParams]
    }

    override def minerAction(sc: SparkContext, hc: HiveContext, inputDF: DataFrame): DataFrame = {
        val scalerAction = params.scalerType match {
            case ScalerType.MIN_MAX => new MinMaxScalerAction(params, debug)
            case ScalerType.STANDARD => new StandardScalerAction(params, debug)
            case ScalerType.Z_INDEX => new ZIndexScalerAction(params, debug)
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

    def setNonScalerField(noNormFieldParam : LinkedHashMap[Int, String]) : Unit = {
        noScalerField = noNormFieldParam.map(x => (Integer.valueOf(x._1), x._2))
    }

    def setJNonScalerFiled(noNormFieldParam : JMap[Integer, String]) : Unit = {
        CollectionUtils.asScalaLinkedHashMap[JMap[Integer, String], LinkedHashMap[Integer, String]](noNormFieldParam, noScalerField)
    }

}

case class StandardScalerAction(params : ScalerFuncParams,
                                override implicit val debug: Boolean = false) extends ScalerTool {
    override def minerAction(sc: SparkContext, hc: HiveContext, input: DataFrame): DataFrame = {
        val libSVMRDD = libSVMHandler.transfer2LibSVM(input, params.noScalerField.toMap)
        val scaler = new StandardScaler(params.meanForStd, params.varianceForStd)
                                        .fit(libSVMRDD.map(x => x._2.features))
        libSVMHandler.transfer2DataFrame(sc, hc, libSVMRDD, params.noScalerField,
                                         input.schema, params.meanForStd)(scaler.transform)
    }
}

case class MinMaxScalerAction(params : ScalerFuncParams,
                              override implicit val debug: Boolean = false) extends ScalerTool {
    override def minerAction(sc: SparkContext, hc: HiveContext, input: DataFrame): DataFrame = {
        val formatStructType = libSVMHandler.genNewDataStructType(input.schema, params.noScalerField, true)
        if (this.debug) {
            println("formatStructType : ")
            formatStructType.fields.foreach(x => println(x.toString()))
        }

        val libSVMRDD = libSVMHandler.transfer2LibSVM(input, params.noScalerField.toMap).map(x => {
            Row(Array(x._2.features).++:(libSVMHandler.restoreDataType(formatStructType.fields, x._1)) : _*)
        })
        if (this.debug) {
            println("format row sample rdd : ")
            libSVMRDD.take(5).foreach(x => println(x.mkString(",")))
        }

        val df = hc.createDataFrame(libSVMRDD, formatStructType)
        if (this.debug) {
            println("df before scaler : !!!")
            df.collect().foreach(x => println(x.mkString(",")))
        }

        val scaler = new MinMaxScaler().setInputCol(INPUT_FEATURES).setOutputCol(OUTPUT_FEATURES)
        val scalerModel = scaler.fit(df)

        var selectNameArr = df.schema.fields.filterNot(_.name == INPUT_FEATURES).map(_.name).toList
        selectNameArr = selectNameArr ::: List(OUTPUT_FEATURES)
        println("---------------")
        println("transform schema: ")
        selectNameArr.foreach(println(_))
        println("---------------")

        val resultRDD = scalerModel.transform(df).selectExpr(selectNameArr.toArray : _ *).map(
            rowHandleAware.vectorRowFlatmapRow(_, OUTPUT_FEATURES)
        )

        if (debug) {
            println("result sample RDD : ")
            resultRDD.take(5).foreach(x => {println(x.mkString(","))})
        }

        val structType = libSVMHandler.genNewDataStructType(input.schema, params.noScalerField, false)
        if (debug) {
            println("result structType : ")
            structType.fields.foreach(x => println(x.toString()))
        }

        hc.createDataFrame(resultRDD, structType)
    }
}

case class ZIndexScalerAction(params : ScalerFuncParams,
                              override implicit val debug: Boolean = false) extends ScalerTool {
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