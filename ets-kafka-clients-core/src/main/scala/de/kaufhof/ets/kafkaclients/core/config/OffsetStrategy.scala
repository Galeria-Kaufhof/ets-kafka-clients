package de.kaufhof.ets.kafkaclients.core.config

import org.apache.kafka.clients.consumer.OffsetResetStrategy

//definies at which offset to start per consumer-group when consuming first time
sealed trait OffsetStrategy {def  strategy: OffsetResetStrategy}

case object FromBeginning extends OffsetStrategy {
  override def  strategy: OffsetResetStrategy = OffsetResetStrategy.EARLIEST
}

case object FromEnd extends OffsetStrategy {
  override def  strategy: OffsetResetStrategy = OffsetResetStrategy.EARLIEST
}
