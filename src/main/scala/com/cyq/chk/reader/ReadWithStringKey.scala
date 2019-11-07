package com.cyq.chk.reader

import com.cyq.chk.WordCount
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.state.api.Savepoint
import com.cyq.chk.base.BaseConstant._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object ReadWithStringKey {

  def main(args: Array[String]): Unit = {

    val executionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // chkPath/$jobId/chk-${checkpointId}
    val localPath = chkPath +  "6a6259e300cb109da4d5c01843906a9a/chk-5"

    val savePoints = Savepoint.load(ExecutionEnvironment.getExecutionEnvironment, localPath, new FsStateBackend(localPath))

    val retState = savePoints.readKeyedState(chkOperatorId, new Reader(executionEnvironment))

    retState.print()
  }
}


class Reader(executionEnvironment: ExecutionEnvironment) extends KeyedStateReaderFunction[String, WordCount] {
  private var state: ValueState[WordCount] = null

  // Here the "_op_state" is from StreamGroupedReduce#STATE_NAME
  val stateProperties = new ValueStateDescriptor[WordCount]("_op_state",
    implicitly[TypeInformation[WordCount]].createSerializer(executionEnvironment.getConfig))

  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(stateProperties)
  }

  override def readKey(e: String, context: KeyedStateReaderFunction.Context, collector: Collector[WordCount]): Unit = {
    println(s"key is $e and value is ${state.value()}")
    collector.collect(WordCount(e.toString,state.value().count))
  }
}

