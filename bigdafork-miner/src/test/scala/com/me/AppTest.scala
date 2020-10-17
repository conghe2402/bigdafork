package com.me

import com.opens.bigdafork.miner.{MTInput, MTOutput, MinerDesc}
import com.opens.bigdafork.miner.tools.lostvalue.CorrectType
import com.opens.bigdafork.miner.tools.lostvalue.CorrectType.CorrectType
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Seq

object AppTest {

    def testKO(): Unit = {
        val conf = new SparkConf().setAppName("test")
        val sc = new SparkContext(conf)
        val hc = new HiveContext(sc)

        val lostTool  = MinerDesc.LostValFuncDesc.getTool()
        val toolParams = MinerDesc.LostValFuncDesc.needParam()

        val lostValueFuncDefSeq = Seq[(Int, String, String, CorrectType)](
            (1, "age", null, CorrectType.MEAN),
            (2, "xscore", "0", CorrectType.MEAN)
        )

        toolParams.setLostValueFuncDef(lostValueFuncDefSeq)

        val input = MTInput("table1", 0)
        val output = MTOutput("table2", 0)

        lostTool.handle(sc, hc, input, output)
        //pa.lostValueDef = "0"
        //lostf.setParams(pa)
        //lostf.handle(null,null,null,null)



    }

    def main(args: Array[String]): Unit = {
        //testKO();
        //val a = new tt()
        //print(a.s)
        print(TT.t)
    }


}

class t {
    val s = this.getClass.getSimpleName
}

class tt extends t {

}


object TT {
    case object t {

        def apply(s : String = "dd") : String = s"nihao ${s}"
    }
}




