package de.kaufhof.ets.kafkaclients.core.config

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.record.CompressionType

/**
  * Please see http://kafka.apache.org/documentation.html#producerconfigs.
  */
case class KafkaProducerConfig(bootstrapServers: String,
                                clientId: ClientId,
                                acks: Acks = Acks.ALL,
                                compressionType: CompressionType = CompressionType.LZ4,
                                retries: Int = 3,
                                enableIdempotence: Boolean = false,
                                maxInFlightRequestsPerConnection: Int = 5) extends KafkaClientConfig {

  override val propertyMap: Map[String, String] = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
    ProducerConfig.CLIENT_ID_CONFIG -> clientId.value,
    ProducerConfig.ACKS_CONFIG -> acks.configValue,
    ProducerConfig.COMPRESSION_TYPE_CONFIG -> compressionType.name,
    ProducerConfig.RETRIES_CONFIG -> retries.toString,
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> enableIdempotence.toString,
    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> maxInFlightRequestsPerConnection.toString
  )

}
