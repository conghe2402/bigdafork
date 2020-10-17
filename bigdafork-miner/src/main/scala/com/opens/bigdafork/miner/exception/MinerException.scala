package com.opens.bigdafork.miner.exception

class MinerException(errorMessages: Seq[String])
  extends RuntimeException {

    override def getMessage: String = {
        if (errorMessages.size == 1) {
            errorMessages.head
        } else {
            errorMessages.zipWithIndex.map { case (msg, i) => s"Exception $i: $msg" }.mkString("\n")
        }
    }
}
