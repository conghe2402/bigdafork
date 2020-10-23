package com.opens.bigdafork.miner.util
import java.util.{Map => JMap}
import scala.collection.mutable.{LinkedHashMap => ScalaMap}

object CollectionUtils {

    /**
      * transfer Java Map to Scala LinkedHashMap.
      * @param jMap
      * @param sMap
      * @tparam J
      * @tparam S
      */
    def asScalaLinkedHashMap[J <: JMap[Integer,String], S <: ScalaMap[Integer, String]](jMap : J, sMap : S) : Unit = {
        val it = jMap.keySet().iterator()
        while (it.hasNext) {
            val key = it.next()
            sMap.+=(key -> jMap.get(key))
        }
    }
}
