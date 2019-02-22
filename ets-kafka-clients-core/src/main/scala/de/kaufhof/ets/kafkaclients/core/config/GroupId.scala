package de.kaufhof.ets.kafkaclients.core.config

trait GroupId {
  val value: String
}

object GroupId {
  def apply(value: String): GroupId = SimpleGroupId(value)
}

case class SimpleGroupId(override val value: String) extends GroupId
