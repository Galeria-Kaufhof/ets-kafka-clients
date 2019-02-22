package de.kaufhof.ets.kafkaclients.test

import java.util.UUID

import de.kaufhof.ets.kafkaclients.core.config._
import org.apache.kafka.clients.consumer.OffsetResetStrategy

trait KafkaConfigTestHelper {

  val bootstrapServers = "172.16.241.10:9092"

  def consumerConfig(groupId: String): KafkaConsumerConfig =
    KafkaConsumerConfig(
      bootstrapServers,
      SimpleGroupId(groupId),
      OffsetResetStrategy.EARLIEST,
      clientId("consumer")
    )

  def producerConfig: KafkaProducerConfig =
    KafkaProducerConfig(
      bootstrapServers,
      clientId("producer")
    )

  def clientId(kafkaClientType: String): ClientId =
    RandomClientId(kafkaClientType + ".test")

  val headers = TestHeaders(UUID.randomUUID())

}
