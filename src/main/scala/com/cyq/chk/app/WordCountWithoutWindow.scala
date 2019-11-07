package com.cyq.chk.app

import java.util.Properties

import com.cyq.chk.WordCount
import org.apache.flink.streaming.api.scala._
import org.apache.commons.lang3.StringUtils
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import com.cyq.chk.base.BaseConstant._
import com.cyq.chk.base.{EnvUtil, KafkaUtil}

object WordCountWithoutWindow {
  def main(args: Array[String]): Unit = {
    val env = EnvUtil.createEnv
    val stream = env
      .addSource(new FlinkKafkaConsumer[String](sourceTopic, new SimpleStringSchema(), KafkaUtil.createConsumerConfigs ))

    val eventSource = stream
      .flatMap(record => StringUtils.split(record, ","))
      .map(WordCount(_,1))

    val sinkStream = eventSource
      .keyBy(_.word)
      .reduce{
        (v1,v2) =>
          WordCount(v1.word,v1.count + v2.count)
      }.uid(chkOperatorId)

    val myProducer2 = new FlinkKafkaProducer(sinkTopic,
      MyKafkaSerialization(sinkTopic),
      KafkaUtil.createProducerConfigs,
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE
    )

    sinkStream.map(r => r.toString).addSink(myProducer2);

    env.execute("WordCount Test")
  }
}
