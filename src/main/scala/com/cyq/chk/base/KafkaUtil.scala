package com.cyq.chk.base

import java.util.Properties

import com.cyq.chk.base.BaseConstant.{brokerServer, groupId}
import org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.{BOOTSTRAP_SERVERS_CONFIG, TRANSACTION_TIMEOUT_CONFIG}

object KafkaUtil {

  def createConsumerConfigs: Properties = {
    val properties = new Properties()
    properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, brokerServer)
    properties.setProperty(GROUP_ID_CONFIG, groupId)
    properties
  }

  def createProducerConfigs : Properties = {
    val props = new Properties()
    props.setProperty(BOOTSTRAP_SERVERS_CONFIG, brokerServer)
    props.setProperty(TRANSACTION_TIMEOUT_CONFIG, String.valueOf(1 * 60 * 1000))
    props
  }

}
