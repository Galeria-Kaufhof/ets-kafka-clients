package de.kaufhof.ets.kafkaclients.core.config

import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetResetStrategy}

/**
  * Please see http://kafka.apache.org/documentation.html#newconsumerconfigs.
  */
case class KafkaConsumerConfig(bootstrapServers: String,
                                groupId: GroupId,
                                autoOffsetReset: OffsetResetStrategy,
                                clientId: ClientId,
                                enableAutoCommit: Boolean = false) extends KafkaClientConfig {

  override val propertyMap: Map[String, String] = Map(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
    ConsumerConfig.GROUP_ID_CONFIG -> groupId.value,
    // Since there is no attribute like "name" as in org.apache.kafka.common.record.CompressionType,
    // this mapping is used. Update if possible.
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> autoOffsetReset.name().toLowerCase,
    ConsumerConfig.CLIENT_ID_CONFIG -> clientId.value,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> enableAutoCommit.toString
  )
}
