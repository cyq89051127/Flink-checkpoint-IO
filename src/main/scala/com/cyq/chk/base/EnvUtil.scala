package com.cyq.chk.base

import com.cyq.chk.base.BaseConstant._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object EnvUtil {

  def createEnv : StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallism)
    env.enableCheckpointing(chkInterval)
    env.setStateBackend(new FsStateBackend(chkPath, true))
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env
  }

}
