package com.opens.bigdafork.miner

import com.opens.bigdafork.miner.MinerDesc._
import com.opens.bigdafork.miner.tools.lostvalue.{LostValFuncParams, LostValFuncTool}
import com.opens.bigdafork.miner.tools.normalize.{NormFuncParams, NormalizerTool}
import com.opens.bigdafork.miner.tools.scaler.{ScalerFuncParams, ScalerTool}

/**
  * The guideline of all kinds of miner tools.
  *
  */
object MinerDesc {

    /**
      * miner desc
      */
    case object LostValFuncDesc extends MinerDesc {
        val name = "lost_value_func"
        val className = "LostValFuncTool"

        // Additionally, define this attr relevant to the specific Miner Func Desc.
        def needParam() : LostValFuncParams = new LostValFuncParams()

        def apply(debug : Boolean = false) : MinerTool = getTool(debug)
    }

    case object TFIDFFuncDesc extends MinerDesc {
        val name = "TF_IDF_func"
        val className = "LostValFuncTool"

    }

    case object NormalizerFuncDesc extends MinerDesc {
        val name = "normalizer_func"
        val className = "NormalizerTool"

        def needParam() : NormFuncParams = new NormFuncParams()
        def apply(debug : Boolean = false) : MinerTool = getTool(debug)
    }

    case object StandardScalerFuncDesc extends MinerDesc {
        val name = "standard_scaler_func"
        val className = "ScalerTool"

        def needParam() : ScalerFuncParams = new ScalerFuncParams()
        def apply(debug : Boolean = false) : MinerTool = getTool(debug)
    }

    case object MinMaxScalerFuncDesc extends MinerDesc {
        val name = "min_max_scaler_func"
        val className = "ScalerTool"

        def needParam() : ScalerFuncParams = new ScalerFuncParams()
        def apply(debug : Boolean = false) : MinerTool = getTool(debug)
    }

    case object ZIndexScalerFuncDesc extends MinerDesc {
        val name = "z_index_scaler_func"
        val className = "ScalerTool"

        def needParam() : ScalerFuncParams = new ScalerFuncParams()
        def apply(debug : Boolean = false) : MinerTool = getTool(debug)
    }

    case object BinarizerFunc extends MinerDesc {
        val name = "binarizer_func"
        val className = "LostValFuncTool"
    }

    case object BucketizerFunc extends MinerDesc {
        val name = "bucketizer_func"
        val className = "LostValFuncTool"
    }

    case object RemoveColsFuncDesc extends MinerDesc {
        val name = "remove_cols_func"
        val className = "LostValFuncTool"
    }
}

object MinerBox {
    def getMinerTool(minerDesc : MinerDesc, debugParam : Boolean = false) : MinerTool = {
        implicit val debug = debugParam
        minerDesc match {
            case LostValFuncDesc => new LostValFuncTool
            case NormalizerFuncDesc => new NormalizerTool
            case StandardScalerFuncDesc => new ScalerTool
            case MinMaxScalerFuncDesc => new ScalerTool
            case ZIndexScalerFuncDesc => new ScalerTool
            case _ => {
                print("invalid")
                null
            }
        }

    }
}

class enum extends scala.annotation.StaticAnnotation;

@enum
sealed trait MinerDesc extends Product with Serializable {
    def name : String
    def className : String
    def getTool() : MinerTool = MinerBox.getMinerTool(this)
    def getTool(debug : Boolean) : MinerTool = {
        MinerBox.getMinerTool(this, debug)
    }
}
