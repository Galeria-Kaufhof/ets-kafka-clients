package de.kaufhof.ets.kafkaclients.core.consumerproducer

import de.kaufhof.ets.kafkaclients.core.config.KafkaConsumerConfig
import de.kaufhof.ets.kafkaclients.core.serialization.protocol._
import de.kaufhof.ets.kafkaclients.core.serialization.{DeSerializerHelper, NoneDeserializer}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

class SimpleKafkaConsumer[Key, Value, Headers, Message <: KafkaMessage[Key, Value, Headers]](
  consumerConfig: KafkaConsumerConfig,
  topicDefinition: KafkaTopicDefinition[Key, Value, Headers, Message]
) {

  private val consumer =
    topicDefinition.protocol match {
      case sp: KafkaStateProtocol[Key, Value, Headers] =>
        new KafkaConsumer[Option[Key], Option[Value]](
          consumerConfig.properties,
          DeSerializerHelper.optionDeserializer(sp.keyDeserializer),
          DeSerializerHelper.optionDeserializer(sp.valueDeserializer)
        )

      case ep: KafkaEventWithKeyProtocol[Key, Value, Headers] =>
        new KafkaConsumer[Option[Key], Option[Value]](
          consumerConfig.properties,
          DeSerializerHelper.optionDeserializer(ep.keyDeserializer),
          DeSerializerHelper.optionDeserializer(ep.valueDeserializer)
        )

      case ep: KafkaEventWithoutKeyProtocol[Value, Headers] =>
        new KafkaConsumer[Option[Key], Option[Value]](
          consumerConfig.properties,
          new NoneDeserializer[Key],
          DeSerializerHelper.optionDeserializer(ep.valueDeserializer)
        )
    }

  private val headersDeserializer = topicDefinition.protocol.headersDeserializer

  private def parseRecord(record: ConsumerRecord[Option[Key], Option[Value]]): Message = {
    val headers = headersDeserializer.deserialize(record.headers())

    val optionalKey = record.key()
    //catch bug in kafka lib: may be null because deserializer was not called => wrap in Option.apply
    val optionalValue = Option(record.value()).flatten

    topicDefinition.protocol.create(optionalKey, optionalValue, headers)
  }

  consumer.subscribe(Iterable(topicDefinition.topicName.name).asJavaCollection)

  @volatile private var shutdownRequested = false

  def startReading(f: Message => Unit,
                   pollTimeout: FiniteDuration = 100.milliseconds): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        while (!shutdownRequested) {
          val records =
            consumer.poll(java.time.Duration.ofMillis(pollTimeout.toMillis))
          if (records.count() > 0) {
            records.asScala.foreach { record =>
              f(parseRecord(record))
            }
            consumer.commitSync()
          }
        }
        consumer.close()
      }
    }).start()
  }

  def stop(): Unit = {
    shutdownRequested = true
  }
}
