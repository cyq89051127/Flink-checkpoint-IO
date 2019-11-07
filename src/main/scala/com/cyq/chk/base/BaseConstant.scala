package com.cyq.chk.base

object BaseConstant {

  val sourceTopic = "topic_in_2"
  val sinkTopic = "topic_out"
  val brokerServer = "10.1.236.67:9092"
  val groupId = "exactly-once-test"

  val chkPath = "file:///tmp/checkpoint/"
  val chkInterval = 60000
  val parallism = 3

  val chkOperatorId = "myUid"

}
