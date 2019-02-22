package de.kaufhof.ets.kafkaclients.akkastream

import java.util

import akka.NotUsed
import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage.CommittableOffset
import akka.kafka.ProducerMessage._
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.scaladsl.{Flow, Keep, Source}
import de.kaufhof.ets.kafkaclients.core.config.{KafkaConsumerConfig, KafkaProducerConfig}
import de.kaufhof.ets.kafkaclients.core.record.KafkaProducerRecord
import de.kaufhof.ets.kafkaclients.core.serialization.{DeSerializerHelper, NullDeserializer, NullSerializer}
import de.kaufhof.ets.kafkaclients.core.serialization.protocol.KafkaMessage._
import de.kaufhof.ets.kafkaclients.core.serialization.protocol.{KafkaMessage, KafkaStateProtocol, KafkaTopicDefinition, _}
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

case class CommittableMessage[Message](committableOffset: CommittableOffset, message: Message)

object AkkaStreamKafkaFactory {

  def producerFlow[Key, Value, Headers, Message <: KafkaMessage[Key, Value, Headers], Passthrough](
    producerConfig: KafkaProducerConfig,
    actorSystem: ActorSystem,
    topicDefinition: KafkaTopicDefinition[Key, Value, Headers, Message]
  ): Flow[(Message, Passthrough), Passthrough, NotUsed] = {

    val flexiFlow = topicDefinition.protocol match {
      case sp: KafkaStateProtocol[Key, Value, Headers] =>

        val producerSettings = AkkaStreamKafkaSettingsFactory.producerSettings(
          producerConfig,
          actorSystem,
          DeSerializerHelper.optionSerializer(sp.keySerializer),
          DeSerializerHelper.optionSerializer(sp.valueSerializer)
        )

        Flow[(Message, Passthrough)]
          .map { case (msg, pt) =>
            val pr = new KafkaProducerRecord(topicDefinition.topicName, sp.headersSerializer.serialize(msg.headers))
              .withKey(msg._key)
              .withValue(msg._value)
              .build()

            Message(pr, pt)
          }
          .viaMat(Producer.flexiFlow(producerSettings))(Keep.right)

      case ep: KafkaEventWithKeyProtocol[Key, Value, Headers] =>
        val producerSettings = AkkaStreamKafkaSettingsFactory.producerSettings(
          producerConfig,
          actorSystem,
          DeSerializerHelper.optionSerializer(ep.keySerializer),
          DeSerializerHelper.optionSerializer(ep.valueSerializer)
        )

        Flow[(Message, Passthrough)]
          .map { case (msg, pt) =>
            val pr = new KafkaProducerRecord(topicDefinition.topicName, ep.headersSerializer.serialize(msg.headers))
              .withKey(msg._key)
              .withValue(msg._value)
              .build()

            Message(pr, pt)
          }
          .viaMat(Producer.flexiFlow(producerSettings))(Keep.right)

      case ep: KafkaEventWithoutKeyProtocol[Value, Headers] =>
        val producerSettings = AkkaStreamKafkaSettingsFactory.producerSettings(
          producerConfig,
          actorSystem,
          new NullSerializer,
          DeSerializerHelper.optionSerializer(ep.valueSerializer)
        )

        Flow[(Message, Passthrough)]
          .map { case (msg, pt) =>
            val pr = new KafkaProducerRecord(topicDefinition.topicName, ep.headersSerializer.serialize(msg.headers))
              .withKey(null)
              .withValue(msg._value)
              .build()

            Message(pr, pt)
          }
          .viaMat(Producer.flexiFlow(producerSettings))(Keep.right)

    }

    flexiFlow.map(_.passThrough)
  }

  def consumerSource[Key, Value, Headers, Message <: KafkaMessage[Key, Value, Headers]](
   consumerConfig: KafkaConsumerConfig,
   actorSystem: ActorSystem,
   topicDefinition: KafkaTopicDefinition[Key, Value, Headers, Message]
  ): Source[CommittableMessage[Message], Consumer.Control] = {

    topicDefinition.protocol match {
      case sp: KafkaStateProtocol[Key, Value, Headers] =>

        val consumerSettings = AkkaStreamKafkaSettingsFactory.consumerSettings(
          consumerConfig,
          actorSystem,
          DeSerializerHelper.optionDeserializer(sp.keyDeserializer),
          DeSerializerHelper.optionDeserializer(sp.valueDeserializer)
        )

        Consumer
          .committableSource(consumerSettings, Subscriptions.topics(topicDefinition.topicName.name))
          .map { msg =>
            val record = msg.record
            val committableOffset = msg.committableOffset

            val headers = sp.headersDeserializer.deserialize(record.headers())
            val optionalKey = record.key()
            //catch bug in kafka lib: may be null because deserializer was not called => wrap in Option.apply
            val optionalValue = Option(record.value()).flatten

            val key =
              optionalKey
                .getOrElse(throw new RuntimeException("Key not found for state"))

            val message: StateMessage[Key, Value, Headers] = optionalValue.map { value =>
              NewState(key, value, headers)
            }.getOrElse {
              DeletedState(key, headers)
            }

            CommittableMessage[Message](
              committableOffset,
              message
            )
          }

      case ep: KafkaEventWithKeyProtocol[Key, Value, Headers] =>
        val consumerSettings = AkkaStreamKafkaSettingsFactory.consumerSettings(
          consumerConfig,
          actorSystem,
          DeSerializerHelper.optionDeserializer(ep.keyDeserializer),
          DeSerializerHelper.optionDeserializer(ep.valueDeserializer)
        )

        Consumer
          .committableSource(consumerSettings, Subscriptions.topics(topicDefinition.topicName.name))
          .map { msg =>
            val record = msg.record
            val committableOffset = msg.committableOffset

            val headers = ep.headersDeserializer.deserialize(record.headers())
            val optionalKey = record.key()

            //catch bug in kafka lib: may be null because deserializer was not called => wrap in Option.apply
            val optionalValue = Option(record.value()).flatten

            val key =
              optionalKey
                .getOrElse(throw new RuntimeException("Key not found for event with key"))

            val value =
              optionalValue
                .getOrElse(throw new RuntimeException("Value not found for event with key"))

            CommittableMessage[Message](
              committableOffset,
              EventWithKey(key, value, headers)
            )
          }

      case ep: KafkaEventWithoutKeyProtocol[Value, Headers] =>
        val consumerSettings = AkkaStreamKafkaSettingsFactory.consumerSettings(
          consumerConfig,
          actorSystem,
          new NullDeserializer,
          DeSerializerHelper.optionDeserializer(ep.valueDeserializer)
        )

        Consumer
          .committableSource(consumerSettings, Subscriptions.topics(topicDefinition.topicName.name))
          .map { msg =>
            val record = msg.record
            val committableOffset = msg.committableOffset

            val headers = ep.headersDeserializer.deserialize(record.headers())

            //catch bug in kafka lib: may be null because deserializer was not called => wrap in Option.apply
            val optionalValue = Option(record.value()).flatten

            val value =
              optionalValue
                .getOrElse(throw new RuntimeException("Value not found for event with key"))

            CommittableMessage[Message](
              committableOffset,
              EventWithoutKey(value, headers)
            )
          }

    }

  }


}
