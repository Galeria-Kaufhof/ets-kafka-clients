package de.kaufhof.ets.kafkaclients.core.serialization.protocol

import de.kaufhof.ets.kafkaclients.core.serialization.headers.{HeadersDeserializer, HeadersSerializer}
import de.kaufhof.ets.kafkaclients.core.serialization.protocol.KafkaMessage.{DeletedState, EventWithKey, EventWithoutKey, NewState}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

sealed trait KafkaMessageProtocol[Key, Value, Headers, +Message <: KafkaMessage[Key, Value, Headers]] {

  private[kafkaclients] def create(keyOpt: Option[Key],
             valueOpt: Option[Value],
             headers: Headers): Message

  private[kafkaclients] val headersDeserializer: HeadersDeserializer[Headers]
  private[kafkaclients] val headersSerializer: HeadersSerializer[Headers]

}

final case class KafkaStateProtocol[Key, Value, Headers](
  keyDeserializer: Deserializer[Key],
  keySerializer: Serializer[Key],
  valueDeserializer: Deserializer[Value],
  valueSerializer: Serializer[Value],
  private[kafkaclients] override val headersDeserializer: HeadersDeserializer[Headers],
  private[kafkaclients] override val headersSerializer: HeadersSerializer[Headers]
) extends KafkaMessageProtocol[Key, Value, Headers, KafkaMessage.StateMessage[Key, Value, Headers]] {

  private[kafkaclients] override def create(
    keyOpt: Option[Key],
    valueOpt: Option[Value],
    headers: Headers
  ): KafkaMessage.StateMessage[Key, Value, Headers] = {
    val key =
      keyOpt.getOrElse(throw new RuntimeException("Key not found for state"))

    valueOpt
      .map(NewState(key, _, headers))
      .getOrElse(DeletedState(key, headers))
  }
}

final case class KafkaEventWithKeyProtocol[Key, Value, Headers](
  keyDeserializer: Deserializer[Key],
  keySerializer: Serializer[Key],
  valueDeserializer: Deserializer[Value],
  valueSerializer: Serializer[Value],
  private[kafkaclients] override val headersDeserializer: HeadersDeserializer[Headers],
  private[kafkaclients] override val headersSerializer: HeadersSerializer[Headers]
) extends KafkaMessageProtocol[Key, Value, Headers, KafkaMessage.EventWithKey[Key, Value, Headers]] {

  private[kafkaclients] override def create(
    keyOpt: Option[Key],
    valueOpt: Option[Value],
    headers: Headers
  ): KafkaMessage.EventWithKey[Key, Value, Headers] = {
    val key =
      keyOpt.getOrElse(throw new RuntimeException("Key not found for event"))

    val value =
      valueOpt.getOrElse(throw new RuntimeException("Value not found for event"))

    EventWithKey(key, value, headers)
  }
}

final case class KafkaEventWithoutKeyProtocol[Value, Headers](
  valueDeserializer: Deserializer[Value],
  valueSerializer: Serializer[Value],
  private[kafkaclients] override val headersDeserializer: HeadersDeserializer[Headers],
  private[kafkaclients] override val headersSerializer: HeadersSerializer[Headers]
) extends KafkaMessageProtocol[Nothing, Value, Headers, KafkaMessage.EventWithoutKey[Value, Headers]] {

  private[kafkaclients] override def create(
    keyOpt: Option[Nothing],
    valueOpt: Option[Value],
    headers: Headers
  ): KafkaMessage.EventWithoutKey[Value, Headers] = {
    val value =
      valueOpt.getOrElse(throw new RuntimeException("Value not found for event"))

    EventWithoutKey(value, headers)
  }
}
