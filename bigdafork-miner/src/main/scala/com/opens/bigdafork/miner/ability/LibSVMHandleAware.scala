package com.opens.bigdafork.miner.ability

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

private[opens]
trait TLibSVMAware {
    val libSVMHandler : TLibSVMHandleAware = LibSVMHandle
}

sealed trait TLibSVMHandleAware extends Serializable {

    /**
      * Transfer normal format data to libSVM format.
      * There are two data types of output :
      * double type for fit data while String type for non-fit data.
      * @param inputDF
      * @param schemaField The field set is excluded from miner computation.
      * @return
      */
    def transfer2LibSVM(inputDF : DataFrame, schemaField : Map[Integer, String]) : RDD[(Array[String], LabeledPoint)]

    /**
      * Transfer RDD to DataFrame.
      * @param sc
      * @param hc
      * @param inputRDD
      * @param keepFields  fields without features computation
      * @param schema original input schema
      * @param toDense true if we need dense vector
      * @param fun
      * @return
      */
    def transfer2DataFrame(sc : SparkContext, hc : HiveContext,
                           inputRDD : RDD[(Array[String], LabeledPoint)],
                           keepFields : LinkedHashMap[Integer, String],
                           schema : StructType, toDense : Boolean = false)
                          (fun : Vector => Vector) : DataFrame


    /**
      * generate new StructType from orginal.
      * @param schema
      * @param keepFields
      * @param format true : feature format cols
      * @return
      */
    def genNewDataStructType(schema : StructType,
      keepFields : mutable.LinkedHashMap[Integer, String], format : Boolean) : StructType


    /**
      * restore values' type from String value by order.
      * @param schemaFields
      * @param values
      * @return
      */
    def restoreDataType(schemaFields : Array[StructField], values : Array[String]) : Array[Any]
}

private[opens]
object LibSVMHandle extends TLibSVMHandleAware {
    def transfer2LibSVM(inputDF : DataFrame, schemaField : Map[Integer, String]) : RDD[(Array[String], LabeledPoint)] = {
        inputDF.map(r => {
            r.toSeq.map(x => x.toString).zipWithIndex.map(x => {
                s"${x._2}:${x._1}"
            })
        }).map {line =>
            val items = line.mkString(",").split(',')
            val label = 0
            var offset = 0
            val (indices, values) = items.filter(_.nonEmpty).map { item =>
                val indexAndValue = item.split(":")
                val index = indexAndValue(0).toInt
                val value = indexAndValue(1)
                var needNormIndex = index - offset

                if (schemaField.contains(index)) {
                    needNormIndex = -1
                    offset += 1
                    (needNormIndex, value)
                } else {
                    (needNormIndex, value.toDouble.toString)
                }
            }.unzip

            // check if indices are one-based and in ascending order
            var previous = -1
            var i = 0
            val indicesLength = indices.length
            while (i < indicesLength) {
                val current = indices(i)
                if (current != -1) {
                    require(current > previous, "indices should be one-based and in ascending order" )
                    previous = current
                }
                i += 1
            }

            (label, indices.toArray, values.toArray)
        }.map { case (label, indices, values) =>
            val needNormValues = ArrayBuffer[Double]()
            val noNormValues = ArrayBuffer[String]()
            for (i <- (0 to indices.length - 1)) {
                if (indices(i) != -1) {
                    needNormValues.+=(values(i).toDouble)
                } else {
                    noNormValues.+=(values(i))
                }
            }

            (noNormValues.toArray, LabeledPoint(label, Vectors.sparse(needNormValues.size, indices.filterNot(_ == -1),
                needNormValues.toArray)))
        }
    }

    /**
      * Transfer RDD to DataFrame.
      * @param sc
      * @param hc
      * @param inputRDD
      * @param keepFields  fields without features computation
      * @param schema original input schema
      * @param toDense true if we need dense vector
      * @param fun
      * @return
      */
    override def transfer2DataFrame(sc : SparkContext, hc : HiveContext,
                                    inputRDD: RDD[(Array[String], LabeledPoint)],
                                    keepFields: mutable.LinkedHashMap[Integer, String],
                                    schema: StructType, toDense : Boolean = false)
                                   (fun : Vector => Vector) : DataFrame = {
        val resultStructType = genNewDataStructType(schema, keepFields, false)
        //vectors.collect().foreach(x => {for (xx <- x._1) {println(xx)}})
        val normRDD = inputRDD.map(x => {(x._1, fun(toDense match {
            case true => Vectors.dense(x._2.features.toArray)
            case _ => x._2.features
        }))}).map(x => {
            Row(x._2.toArray.++:(restoreDataType(resultStructType.fields, x._1)) : _*)
        })

        println("result RDD:")
        val sample = normRDD.take(10)
        sample.foreach(x => {println(x.mkString(","))})
        sample.foreach(x => {println(s"len : ${x.size} : st len : ${resultStructType.size}")})

        hc.createDataFrame(normRDD, resultStructType)
    }

    override def genNewDataStructType(schema : StructType,
                                      keepFields : mutable.LinkedHashMap[Integer, String],
                                      format : Boolean) : StructType = {
        val fields = ArrayBuffer[StructField]()
        val schemaFields = schema.fields
        val normFields = ArrayBuffer[StructField]()
        for (i <- (0 to schemaFields.size - 1)) {
            if (keepFields.contains(i)) {
                fields.+=(schemaFields(i))
            } else {
                normFields.+=(DataTypes.createStructField(schemaFields(i).name, DataTypes.DoubleType, true))
            }
        }

        if (format) {
            val vectorType = classOf[VectorUDT].newInstance()
            fields.+=(DataTypes.createStructField("features", vectorType, true))
        } else {
            fields.++=(normFields)
        }

        DataTypes.createStructType(fields.toArray)
    }

    def restoreDataType(schemaFields : Array[StructField], values : Array[String]) : Array[Any] = {
        val newFields = ArrayBuffer[Any]()
        for (i <- (0 to values.length - 1)) {
            schemaFields(i).dataType match {
                case DataTypes.IntegerType => newFields.+=(values(i).toInt)
                case DataTypes.StringType => newFields.+=(values(i).toString)
                case DataTypes.DoubleType => newFields.+=(values(i).toDouble)
                case _ => newFields.+=(values(i).toString)
            }
        }
        newFields.toArray
    }


}