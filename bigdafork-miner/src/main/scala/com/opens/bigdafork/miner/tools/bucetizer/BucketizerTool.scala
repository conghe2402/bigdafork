package com.opens.bigdafork.miner.tools.bucetizer

import com.opens.bigdafork.miner.{MTParams, MinerTool}
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.col

import java.util.{List => JList}

import scala.collection.mutable.Seq
import scala.collection.JavaConversions._

class BucketizerTool (implicit debug : Boolean = false) extends MinerTool(debug) {

    private var params: BucketizerFuncParams = null

    override def setParams(para: MTParams) : Unit = {
        this.params = para.asInstanceOf[BucketizerFuncParams]
    }

    override def minerAction(sc: SparkContext, hc: HiveContext, inputDF: DataFrame): DataFrame = {
        var dataFrame = inputDF
        val bucketizer = new Bucketizer()
        for (bf <- params.buckFields) {
            bucketizer.setInputCol(bf._2).setOutputCol(s"${bf._2}_${bf._1}")
                      .setSplits(setSplits(bf._3, bf._4, bf._5))
            dataFrame = bucketizer.transform(dataFrame).withColumn(bf._2,
                                                                   col(s"${bf._2}_${bf._1}"))
                                                       .drop(s"${bf._2}_${bf._1}")
        }
        dataFrame
    }

    private def setSplits(splitParams : Array[Double], downBound : Boolean, upBound : Boolean) : Array[Double] = {
        if (downBound && !upBound) {
            Array(Double.NegativeInfinity) ++ splitParams
        } else if (!downBound && upBound) {
            splitParams ++ Array(Double.PositiveInfinity)
        } else {
            Array(Double.NegativeInfinity) ++ splitParams ++ Array(Double.PositiveInfinity)
        }
    }
}


/**
  * params.
  */
class BucketizerFuncParams() extends MTParams {
    //col id, col name, splits in asc order, down bound, up bound
    var buckFields : Seq[(Int, String, Array[Double], Boolean, Boolean)] = null
    def setBuckFields(buckFieldsParam : Seq[(Int, String, Array[Double], Boolean, Boolean)]) : Unit = {
        buckFields = buckFieldsParam
    }

    def setJBuckFields(buckFieldsParam : JList[(Integer, String, JList[java.lang.Double],
                                                java.lang.Boolean, java.lang.Boolean)]) : Unit = {
        buckFields = buckFieldsParam.map(a => {
            Tuple5(Option(a._1).map(_.toInt).get, a._2,
                a._3.map(x => Option(x).map(_.toDouble).get).toArray,
                Boolean.unbox(a._4),
                Boolean.unbox(a._5))
        })
    }

}
