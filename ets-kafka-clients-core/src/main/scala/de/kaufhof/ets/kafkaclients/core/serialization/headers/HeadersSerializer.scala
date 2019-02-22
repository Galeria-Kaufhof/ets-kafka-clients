package de.kaufhof.ets.kafkaclients.core.serialization.headers

trait HeadersSerializer[Headers] {
  def serialize(headers: Headers): org.apache.kafka.common.header.Headers
}
