package de.kaufhof.ets.kafkaclients.core.serialization

import java.util

import org.apache.kafka.common.serialization.Deserializer

class NullDeserializer extends Deserializer[Null] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def close(): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): Null = null
}
