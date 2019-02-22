package de.kaufhof.ets.kafkaclients.core.serialization.protocol

import de.kaufhof.ets.kafkaclients.core.topic.{TopicName, VersionedTopic}

import scala.reflect.ClassTag

abstract class KafkaTopicDefinition[Key, Value, Header, Message <: KafkaMessage[Key, Value, Header]] {
  def protocol: KafkaMessageProtocol[Key, Value, Header, Message]
  def topicName: TopicName
}

abstract class VersionedKafkaTopicDefinition[Key, Value: ClassTag, Header, Message <: KafkaMessage[Key, Value, Header]]
  extends KafkaTopicDefinition[Key, Value, Header, Message] {
  override def topicName: TopicName = VersionedTopic[Value]
}
