package com.cyq.chk.app

import com.cyq.chk.WordCount
import com.cyq.chk.base.BaseConstant.{chkOperatorId, sinkTopic, sourceTopic}
import com.cyq.chk.base.{EnvUtil, KafkaUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.api.scala._

object WordCountWithoutWindowKeybyString {
  def main(args: Array[String]): Unit = {
    val env = EnvUtil.createEnv
    val stream = env
      .addSource(new FlinkKafkaConsumer[String](sourceTopic, new SimpleStringSchema(), KafkaUtil.createConsumerConfigs ))

    val eventSource = stream
      .flatMap(record => StringUtils.split(record, ","))
      .map(WordCount(_,1))


    val sinkStream = eventSource
      // Here we use the name as as the keyBy parameter, we need use ReadWithTupleStringKey to read the checkpoint
      .keyBy("word")
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
