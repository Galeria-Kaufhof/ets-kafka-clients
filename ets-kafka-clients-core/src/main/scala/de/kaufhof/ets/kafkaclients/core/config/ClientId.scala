package de.kaufhof.ets.kafkaclients.core.config

import java.util.UUID

trait ClientId {
  val value: String
}

object ClientId {
  def apply(value: String): ClientId = RandomClientId(value)
}

case class SimpleClientId(override val value: String) extends ClientId
case class RandomClientId(name: String = "") extends ClientId {
  override val value: String = (if (name.nonEmpty) name + "." else "") + UUID.randomUUID().toString
}