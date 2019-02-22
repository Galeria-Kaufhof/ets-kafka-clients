package de.kaufhof.ets.kafkaclients.core.serialization

import java.util

import org.apache.kafka.common.serialization.Serializer

class NullSerializer extends Serializer[Null] {
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, data: Null): Null = null

  override def close(): Unit = ()
}
