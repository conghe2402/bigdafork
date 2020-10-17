package com.opens.bigdafork.miner

import com.opens.bigdafork.miner.MinerDesc.LostValFuncDesc
import com.opens.bigdafork.miner.tools.lostvalue.{LostValFuncParams, LostValFuncTool}

/**
  * The guideline of all kinkds of miner tools.
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

        def apply(debug : Boolean = false): MinerTool = getTool()
    }

    case object TFIDFFuncDesc extends MinerDesc {
        val name = "TF_IDF_func"
        val className = "LostValFuncTool"
    }

    case object NormalizerFuncDesc extends MinerDesc {
        val name = "normalizer_func"
        val className = "LostValFuncTool"
    }

    case object StandardScalerFuncDesc extends MinerDesc {
        val name = "standard_scaler_func"
        val className = "LostValFuncTool"
    }

    case object MinMaxScalerFuncDesc extends MinerDesc {
        val name = "min_max_scaler_func"
        val className = "LostValFuncTool"
    }

    case object MaxAbsScalerFuncDesc extends MinerDesc {
        val name = "max_abs_scaler_func"
        val className = "LostValFuncTool"
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
    def getMinerTool(minerDesc : MinerDesc, debug : Boolean = false) : MinerTool = minerDesc match {
        case LostValFuncDesc => new LostValFuncTool(debug)
        case _ => {
            print("invalid")
            null
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
