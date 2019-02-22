package de.kaufhof.ets.kafkaclients.core.serialization

import java.util

import org.apache.kafka.common.serialization.Serializer

class AsNullSerializer[A] extends Serializer[A] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
  override def serialize(topic: String, data: A): Array[Byte] = null
  override def close(): Unit = ()
}
