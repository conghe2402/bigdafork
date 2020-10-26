package com.opens.bigdafork.miner.ability

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

private[opens]
trait TDataFrameRowAware {
    val rowHandleAware : TRowHandleAware = RowHandleAware
}

sealed trait TRowHandleAware extends Serializable {

    /**
      * A Row with a vector field flat map to row without any vector.
      * @param x
      * @param vName
      * @return
      */
    def vectorRowFlatmapRow(x : Row, vName : String) : Row

}

private[opens]
object RowHandleAware extends TRowHandleAware {
    /**
      * A Row with a vector field flat map to row without any vector.
      *
      * @param x
      * @return
      */
    override def vectorRowFlatmapRow(x: Row, vName : String) = {
        val y = x.schema.foldLeft(ArrayBuffer[Any]())((a, f) => {
            if (f.dataType == IntegerType) {
                a.+=(x.getInt(x.fieldIndex(f.name)))
            } else if (f.dataType == StringType) {
                a.+=(x.getString(x.fieldIndex(f.name)))
            } else if (f.dataType == DoubleType) {
                a.+=(x.getDouble(x.fieldIndex(f.name)))
            } else if (f.name == "scaledFeatures") {
                x.getAs[Vector](f.name).toArray.foreach(a.+=(_))
            }
            a
        })
        Row(y.toArray : _ *)

    }
}
