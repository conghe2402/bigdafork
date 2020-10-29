package com.opens.bigdafork.miner.tools.lostvalue

import com.opens.bigdafork.miner.exception.MinerException
import com.opens.bigdafork.miner.tools.lostvalue.CorrectType.CorrectType
import com.opens.bigdafork.miner.{MTParams, MinerTool}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{coalesce, lit, nanvl, when, trim, round, avg}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext
import java.util.{List => JList}

import scala.collection.mutable.{ListBuffer, Seq, Map => MMap}
import scala.collection.JavaConversions._

/**
  * Tool.
  * drop -> lost value replace -> compute lost value -> fill.
  */
class LostValFuncTool(implicit debug : Boolean = false) extends MinerTool(debug) {

    private var params: LostValFuncParams = null

    override def setParams(para: MTParams) : Unit = {
        this.params = para.asInstanceOf[LostValFuncParams]
    }

    override def minerAction(sc: SparkContext, hc: HiveContext, inputDF: DataFrame): DataFrame = {
        // table
        val cols = inputDF.columns

        // check
        assert (params.lostValueFuncDefSeq != null && !params.lostValueFuncDefSeq.isEmpty)
        val invalidParams = params.lostValueFuncDefSeq.find(t => {
            if (t._1 < cols.length) {
                cols(t._1) != t._2
            } else true
        })
        if (invalidParams != null && invalidParams.size > 0) {
            throw new MinerException(for (p <- invalidParams.toSeq)
                yield s"Invalid lost value func definition : ${p._1} ${p._2}. " +
                  s"Check if index and name are matched")
        }

        // drop...

        // replace
        val lostValueFillFuncList = params.lostValueFuncDefSeq.filter(d => {
            // exclude drop operation func
            true
        })
        // return : no FillFunc or no lost value
        if (lostValueFillFuncList.isEmpty) {
            print("exit, no need to fill lost value")
            return null
        }

        val replaceFields = lostValueFillFuncList.filter(t => {
            t._3 != null
        })

        if (replaceFields.isEmpty) {
            println("no need to replace lost value.")
        }

        var replaceDF = inputDF
        for (f <- replaceFields) {
            println(s"replace lost value '${f._3}' of field ${f._2}")
            getColType(inputDF, f._2).dataType match {
                case StringType => {
                    replaceDF = replaceDF.withColumn(f._2,
                        when(trim(inputDF.col(addApos(f._2))) === f._3, null)
                                 .otherwise(inputDF.col(addApos(f._2))))
                }
                case _ => {
                    replaceDF = replaceDF.withColumn(f._2,
                        when(inputDF.col(addApos(f._2)) === f._3, null)
                                 .otherwise(inputDF.col(addApos(f._2))))
                }
            }
        }

        if (this.debug && replaceFields.size > 0) {
            println("replace data: ")
            printSampleData(replaceDF)
        }

        // calculate
        val filedLostValueMap = for (d <- lostValueFillFuncList) yield { d._2 -> 0d }

        val appendExprsList = ListBuffer[Column]()
        val firstField = lostValueFillFuncList(0)
        for (ff <- lostValueFillFuncList.filterNot(_._2 == firstField._2)) {
            appendExprsList.append(mapExpr(ff))
        }

        val statisticsDF = replaceDF.agg(mapExpr(firstField),
                                            appendExprsList.toArray[Column]: _*)

        val statistics = statisticsDF.collect().head
        if (debug) {
            println("calc statistics row : ")
            println(statistics)
        }

        val lostValFillFuncMap = lostValueFillFuncList.foldRight[MMap[String, CorrectType]](MMap())((t, m) => {
            m.+=((t._2 -> t._4))
        })
        val values = filedLostValueMap.foldRight[MMap[String, String]](MMap())((r, m) => {
            // val colType = getColType(df, r._1)
            // The method to get statistics value depends on CorrectType.
            if (lostValFillFuncMap(r._1) == CorrectType.MEAN) {
                m.+=((r._1 -> statistics.getDouble(statistics.fieldIndex(r._1)).toString))
            } else {
                m.+=((r._1 -> statistics.getString(statistics.fieldIndex(r._1))))
            }
        })
        println(s"calc fill value: \n ${values}")

        // fill
        for (f <- lostValueFillFuncList) {
            replaceDF = replaceDF.withColumn(f._2,
                fill(replaceDF, getColType(inputDF, f._2), values.get(f._2).get))
        }

        replaceDF
    }

    private def fill(df : DataFrame, col: StructField, replacement : String) : Column = {
        if (col.dataType.isInstanceOf[StringType]) {
            fillCol[String](df, col, replacement)
        } else {
            fillCol[Double](df, col, replacement.toDouble)
        }

    }

    private def fillCol[T](df : DataFrame, col: StructField, replacement: T): Column = {
        if (debug) println(s"fill value ${replacement} in col ${col.name}")
        col.dataType match {
            case DoubleType | FloatType =>
                coalesce(nanvl(df.col(addApos(col.name)), lit(null)),
                    lit(replacement).cast(col.dataType)).as(col.name)
            case _ =>
                coalesce(df.col(addApos(col.name)),
                    lit(replacement).cast(col.dataType)).as(col.name)

        }
    }

    private def getColType(df : DataFrame, columnName : String) : StructField = {
        val option = df.schema.fields.find(st => {
            st.name == columnName
        })
        option.getOrElse({
            throw new MinerException(Seq(s"Can not find ${columnName}`s type"))
        })
    }

    private def mapExpr(lostValFuncDesc: (Int, String, String, CorrectType)) : Column = {
        lostValFuncDesc._4 match {
            case CorrectType.MEAN => {
                round(avg(addApos(lostValFuncDesc._2)), 2).as(lostValFuncDesc._2)
            }
            case _ => throw new MinerException(Seq(s"func ${lostValFuncDesc._4} is " +
                                        s"not support on filed ${lostValFuncDesc._2}"))
        }
    }

    private def getStructFieldType(t : String) : DataType = ???

    private def dropRow() : DataFrame = ???

    private def dropCol() : DataFrame = ???
}

/**
  * params.
  */
class LostValFuncParams extends MTParams {
    //field index base on 0, field name, field lost value def, correct method.
    // default lost value defs are NaN and null.
    var lostValueFuncDefSeq : Seq[(Int, String, String, CorrectType)] = null
    def setLostValueFuncDef(lostValFuncDefSeq : Seq[(Int, String, String, CorrectType)]) = {
        this.lostValueFuncDefSeq = lostValFuncDefSeq
    }
    def setJLostValueFuncDef(lostValFuncDefSeq : JList[(Integer, String, String, CorrectType)]) = {
        this.lostValueFuncDefSeq = lostValFuncDefSeq.map(a => {
            Tuple4(Option(a._1).map(_.toInt).get, a._2, a._3, a._4)
        })
    }
}

object CorrectType extends Enumeration {
    type CorrectType = Value
    val MEAN = Value(0, "mean")
    val MODE = Value(1, "mode")
    //val DROP_ROW = Value(1, "drop row")
    //val DROP_COL = Value(2, "drop col")
}


