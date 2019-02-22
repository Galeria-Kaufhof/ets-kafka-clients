package de.kaufhof.ets.kafkaclients.core.config

import java.util.Properties

private[config] trait KafkaClientConfig {

  val propertyMap: Map[String, String]

  def properties: Properties = {
    val props = new Properties()

    propertyMap.foreach { case (key, value) =>
      props.put(key, value)
    }

    props
  }
}
