package de.kaufhof.ets.kafkaclients.akkastream

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerSettings}
import de.kaufhof.ets.kafkaclients.core.config.{KafkaConsumerConfig, KafkaProducerConfig}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

object AkkaStreamKafkaSettingsFactory {
  def producerSettings[K, V](producerConfig: KafkaProducerConfig, actorSystem: ActorSystem, keySerializer: Serializer[K], valueSerializer: Serializer[V]): ProducerSettings[K, V] =
    ProducerSettings(
      actorSystem,
      keySerializer,
      valueSerializer
    ).withProperties(producerConfig.propertyMap)

  def consumerSettings[K, V](consumerConfig: KafkaConsumerConfig, actorSystem: ActorSystem, keyDeserializer: Deserializer[K], valueDeserializer: Deserializer[V]): ConsumerSettings[K, V] =
    ConsumerSettings(
      actorSystem,
      keyDeserializer,
      valueDeserializer
    ).withProperties(consumerConfig.propertyMap)
}
