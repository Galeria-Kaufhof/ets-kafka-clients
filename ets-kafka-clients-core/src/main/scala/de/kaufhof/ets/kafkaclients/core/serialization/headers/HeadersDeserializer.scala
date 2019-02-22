package de.kaufhof.ets.kafkaclients.core.serialization.headers

trait HeadersDeserializer[Headers] {
  def deserialize(headers: org.apache.kafka.common.header.Headers): Headers
}
