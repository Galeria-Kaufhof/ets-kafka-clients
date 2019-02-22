package de.kaufhof.ets.kafkaclients.core.serialization

import java.util

import org.apache.kafka.common.serialization.Deserializer

class NoneDeserializer[A] extends Deserializer[Option[A]] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  override def deserialize(topic: String, data: Array[Byte]): Option[A] = None
  override def close(): Unit = ()
}
