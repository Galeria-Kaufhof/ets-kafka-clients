package de.kaufhof.ets.kafkaclients.test

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import de.kaufhof.ets.kafkaclients.akkastream.AkkaStreamKafkaFactory
import de.kaufhof.ets.kafkaclients.core.config._
import de.kaufhof.ets.kafkaclients.core.serialization.protocol.{KafkaMessage, KafkaTopicDefinition}

import scala.concurrent.Await
import scala.concurrent.duration._

trait KafkaTestHelper extends KafkaConfigTestHelper {

  implicit val as = ActorSystem()
  implicit val mat = ActorMaterializer()

  import as.dispatcher

  def defaultProducerConfig(serviceName: String): KafkaProducerConfig = {
    KafkaProducerConfig(
      bootstrapServers = bootstrapServers,
      RandomClientId(serviceName)
    )
  }

  def defaultConsumerConfig(serviceName: String, offsetStrategy: OffsetStrategy = FromBeginning): KafkaConsumerConfig = {
    KafkaConsumerConfig(
      bootstrapServers = bootstrapServers,
      SimpleGroupId(serviceName),
      offsetStrategy.strategy,
      RandomClientId(serviceName)
    )
  }

  def producerFlow[K, V, H, Msg <: KafkaMessage[K, V, H]](topicDef: KafkaTopicDefinition[K, V, H, Msg]) =
    AkkaStreamKafkaFactory.producerFlow[K, V, H, Msg, Msg](
      defaultProducerConfig("testservice"),
      as,
      topicDef
    )

  def consumerSource[K, V, H, Msg <: KafkaMessage[K, V, H]](topicDef: KafkaTopicDefinition[K, V, H, Msg]) =
    AkkaStreamKafkaFactory.consumerSource(
      defaultConsumerConfig("testservice"),
      as,
      topicDef
    )

  def produceAndConsume[K, V, H, Msg <: KafkaMessage[K, V, H]](msgs: List[Msg], topicDef: KafkaTopicDefinition[K, V, H, Msg]): List[Msg] = {
    val writeToKafka = Source(msgs.map(x => (x,x)))
      .via(producerFlow(topicDef))
      .runWith(Sink.ignore)

    val readFromKafka =
      consumerSource(topicDef)
        .map{msg =>
          msg.committableOffset.commitScaladsl()
          msg.message
        }
        .take(msgs.size)
        .runWith(Sink.seq)
        .map(_.toList)

    Await.result(writeToKafka.flatMap(_ => readFromKafka), 10.seconds)
  }
}
