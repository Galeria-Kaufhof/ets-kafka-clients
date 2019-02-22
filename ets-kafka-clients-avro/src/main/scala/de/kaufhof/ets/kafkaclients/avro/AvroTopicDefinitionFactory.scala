package de.kaufhof.ets.kafkaclients.avro

import de.kaufhof.ets.kafkaclients.avro.serialization.{AvroJsonDeserializer, AvroJsonSerializer}
import de.kaufhof.ets.kafkaclients.core.serialization.headers.{HeadersDeserializer, HeadersSerializer}
import de.kaufhof.ets.kafkaclients.core.serialization.protocol.KafkaMessage.{EventWithKey, EventWithoutKey, StateMessage}
import de.kaufhof.ets.kafkaclients.core.serialization.protocol._
import de.kaufhof.ets.kafkaclients.core.topic.{TopicName, VersionedTopic}
import org.apache.avro.specific.SpecificRecordBase

import scala.reflect.ClassTag

object AvroTopicDefinitionFactory {

  object Versioned {

    def stateTopicDefinition[Key <: SpecificRecordBase : ClassTag, Value <: SpecificRecordBase : ClassTag, Headers](headersDeserializer: HeadersDeserializer[Headers], headersSerializer: HeadersSerializer[Headers]): KafkaTopicDefinition[Key, Value, Headers, StateMessage[Key, Value, Headers]] =
      AvroTopicDefinitionFactory.stateTopicDefinition(VersionedTopic[Value], headersDeserializer, headersSerializer)

    def eventWithKeyTopicDefinition[Key <: SpecificRecordBase : ClassTag, Value <: SpecificRecordBase : ClassTag, Headers](headersDeserializer: HeadersDeserializer[Headers], headersSerializer: HeadersSerializer[Headers]): KafkaTopicDefinition[Key, Value, Headers, EventWithKey[Key, Value, Headers]] =
      AvroTopicDefinitionFactory.eventWithKeyTopicDefinition(VersionedTopic[Value], headersDeserializer, headersSerializer)

    def eventWithoutKeyTopicDefinition[Value <: SpecificRecordBase : ClassTag, Headers](headersDeserializer: HeadersDeserializer[Headers], headersSerializer: HeadersSerializer[Headers]): KafkaTopicDefinition[Nothing, Value, Headers, EventWithoutKey[Value, Headers]] =
      AvroTopicDefinitionFactory.eventWithoutKeyTopicDefinition(VersionedTopic[Value], headersDeserializer, headersSerializer)
  }

  def stateTopicDefinition[Key <: SpecificRecordBase : ClassTag, Value <: SpecificRecordBase : ClassTag, Headers](topicName: TopicName, headersDeserializer: HeadersDeserializer[Headers], headersSerializer: HeadersSerializer[Headers]): KafkaTopicDefinition[Key, Value, Headers, StateMessage[Key, Value, Headers]] =
    kafkaTopicDefinition(
      topicName,
      new KafkaStateProtocol[Key, Value, Headers](
        new AvroJsonDeserializer[Key](),
        new AvroJsonSerializer[Key](),
        new AvroJsonDeserializer[Value](),
        new AvroJsonSerializer[Value](),
        headersDeserializer,
        headersSerializer
      )
    )

  def eventWithKeyTopicDefinition[Key <: SpecificRecordBase : ClassTag, Value <: SpecificRecordBase : ClassTag, Headers](topicName: TopicName, headersDeserializer: HeadersDeserializer[Headers], headersSerializer: HeadersSerializer[Headers]): KafkaTopicDefinition[Key, Value, Headers, EventWithKey[Key, Value, Headers]] =
    kafkaTopicDefinition(
      topicName,
      new KafkaEventWithKeyProtocol[Key, Value, Headers](
        new AvroJsonDeserializer[Key](),
        new AvroJsonSerializer[Key](),
        new AvroJsonDeserializer[Value](),
        new AvroJsonSerializer[Value](),
        headersDeserializer,
        headersSerializer
      )
    )

  def eventWithoutKeyTopicDefinition[Value <: SpecificRecordBase : ClassTag, Headers](topicName: TopicName, headersDeserializer: HeadersDeserializer[Headers], headersSerializer: HeadersSerializer[Headers]): KafkaTopicDefinition[Nothing, Value, Headers, EventWithoutKey[Value, Headers]] =
    kafkaTopicDefinition[Nothing, Value, Headers, EventWithoutKey[Value, Headers]](
      topicName,
      new KafkaEventWithoutKeyProtocol[Value, Headers](
        new AvroJsonDeserializer[Value](),
        new AvroJsonSerializer[Value](),
        headersDeserializer,
        headersSerializer
      )
    )

  private def kafkaTopicDefinition[Key, Value, Headers, Message <: KafkaMessage[Key, Value, Headers]](_topicName: TopicName, f: KafkaMessageProtocol[Key, Value, Headers, Message]): KafkaTopicDefinition[Key, Value, Headers, Message] = {
    new KafkaTopicDefinition[Key, Value, Headers, Message] {
      override def protocol: KafkaMessageProtocol[Key, Value, Headers, Message] = f
      override def topicName: TopicName = _topicName
    }
  }

}
