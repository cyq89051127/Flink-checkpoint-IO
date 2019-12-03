package com.cyq.chk.reader

import com.cyq.chk.WordCount
import com.cyq.chk.base.BaseConstant.{chkOperatorId, chkPath}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.tuple.Tuple1
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.state.api.Savepoint
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object ReadWithTupleStringKey {
  val executionEnvironment = ExecutionEnvironment.getExecutionEnvironment

  // chkPath/$jobId/chk-${checkpointId}
  val localPath = chkPath +  "6a6259e300cb109da4d5c01843906a9a/chk-5"

  val savePoints = Savepoint.load(ExecutionEnvironment.getExecutionEnvironment, localPath, new FsStateBackend(localPath))

  val retState = savePoints.readKeyedState(chkOperatorId, new TupleChkReader(executionEnvironment))

  retState.print()
}

class TupleChkReader(executionEnvironment: ExecutionEnvironment) extends KeyedStateReaderFunction[Tuple1[String], WordCount] {

  private var state: ValueState[WordCount] = _

  // Here the "_op_state" is from StreamGroupedReduce#STATE_NAME
  val stateProperties = new ValueStateDescriptor[WordCount]("_op_state",
    implicitly[TypeInformation[WordCount]].createSerializer(executionEnvironment.getConfig))

  override def open(parameters: Configuration): Unit = {
    state = getRuntimeContext.getState(stateProperties)
  }

  override def readKey(e: Tuple1[String], context: KeyedStateReaderFunction.Context, collector: Collector[WordCount]): Unit = {

    println(s"key is $e and value is ${state.value()}")
    collector.collect(WordCount(e.toString,state.value().count))
  }
}

