package com.cyq.chk.app

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord

class MyKafkaSerialization[V](topic : java.lang.String) extends KafkaSerializationSchema[V] {


  override def serialize(value: V,  aLong: java.lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
     new ProducerRecord[Array[Byte], Array[Byte]](topic, value.toString.getBytes)
  }
}
////
object MyKafkaSerialization {
  def apply(topic: java.lang.String): MyKafkaSerialization[String] = new MyKafkaSerialization(topic)
}
