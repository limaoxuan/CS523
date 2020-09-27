package com.max.bigdataprocessing.controller.streaming.utils

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer

object ParamsConf {
  private lazy val config = ConfigFactory.load()
  val topic = config.getString("kafka.topic").split(",")
  val groupId = config.getString("kafka.group.id")
  val brokers = config.getString("kafka.broker.list")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "192.168.8.178:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "streaming_kafka_group",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  def main(args: Array[String]): Unit = {
   println(ParamsConf.topic)
  }
}
