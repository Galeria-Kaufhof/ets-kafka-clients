package de.kaufhof.ets.kafkaclients.core.serialization.protocol

sealed trait KafkaMessage[Key, +Value, Headers] {
  private[kafkaclients] def _key: Option[Key]
  private[kafkaclients] def _value: Option[Value]
  def headers: Headers
}

object KafkaMessage {
  sealed trait StateMessage[Key, +Value, Headers] extends KafkaMessage[Key, Value, Headers]

  final case class NewState[Key, Value, Headers](key: Key, value: Value, headers: Headers) extends StateMessage[Key, Value, Headers] {
    override private[kafkaclients] def _key = Some(key)
    override private[kafkaclients] def _value = Some(value)
  }

  final case class DeletedState[Key, Headers](key: Key, headers: Headers) extends StateMessage[Key, Nothing, Headers] {
    override private[kafkaclients] def _key = Some(key)
    override private[kafkaclients] def _value = None
  }

  final case class EventWithKey[Key, Value, Headers](key: Key, value: Value, headers: Headers) extends KafkaMessage[Key, Value, Headers] {
    override private[kafkaclients] def _key = Some(key)
    override private[kafkaclients] def _value = Some(value)
  }

  final case class EventWithoutKey[Value, Headers](value: Value, headers: Headers) extends KafkaMessage[Nothing, Value, Headers] {
    override private[kafkaclients] def _key = None
    override private[kafkaclients] def _value = Some(value)
  }
}
