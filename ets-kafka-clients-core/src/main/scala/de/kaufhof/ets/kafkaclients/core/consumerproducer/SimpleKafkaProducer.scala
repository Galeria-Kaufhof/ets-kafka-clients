package de.kaufhof.ets.kafkaclients.core.consumerproducer

import java.util.concurrent.TimeUnit

import de.kaufhof.ets.kafkaclients.core.config.KafkaProducerConfig
import de.kaufhof.ets.kafkaclients.core.record.KafkaProducerRecord
import de.kaufhof.ets.kafkaclients.core.serialization.protocol._
import de.kaufhof.ets.kafkaclients.core.serialization.{AsNullSerializer, DeSerializerHelper}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, RecordMetadata}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}

class SimpleKafkaProducer[Key, Value, Headers, Message <: KafkaMessage[Key, Value, Headers]](
  producerConfig: KafkaProducerConfig,
  topicDefinition: KafkaTopicDefinition[Key, Value, Headers, Message]
) {

  private val kafkaProducer: KafkaProducer[Option[Key], Option[Value]] =
    topicDefinition.protocol match {
      case sp: KafkaStateProtocol[Key, Value, Headers] =>

        new KafkaProducer(
          producerConfig.properties,
          DeSerializerHelper.optionSerializer(sp.keySerializer),
          DeSerializerHelper.optionSerializer(sp.valueSerializer)
        )

      case ep: KafkaEventWithKeyProtocol[Key, Value, Headers] =>
        new KafkaProducer(
          producerConfig.properties,
          DeSerializerHelper.optionSerializer(ep.keySerializer),
          DeSerializerHelper.optionSerializer(ep.valueSerializer)
        )

      case ep: KafkaEventWithoutKeyProtocol[Value, Headers] =>
        new KafkaProducer(
          producerConfig.properties,
          DeSerializerHelper.optionSerializer(new AsNullSerializer[Key]),
          DeSerializerHelper.optionSerializer(ep.valueSerializer)
        )
    }

  private val headersSerializer = topicDefinition.protocol.headersSerializer

  def produce(msg: Message): Future[RecordMetadata] = {
    val p = Promise[RecordMetadata]()

    val record =
      new KafkaProducerRecord[Option[Key], Option[Value]](topicDefinition.topicName, headersSerializer.serialize(msg.headers))
        .withKey(msg._key)
        .withValue(msg._value)
        .build()

    kafkaProducer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception == null) p.success(metadata)
        else p.failure(exception)
      }
    })

    p.future
  }

  def close(finiteDuration: FiniteDuration): Unit = {
    kafkaProducer.close(finiteDuration.toMillis, TimeUnit.MILLISECONDS)
  }

}
