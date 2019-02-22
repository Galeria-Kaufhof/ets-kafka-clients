package de.kaufhof.ets.kafkaclients.core.serialization

import java.util

import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.{Deserializer, Serializer}

object DeSerializerHelper {

  def optionSerializer[A](
                                   serializer: Serializer[A]
                                 ): Serializer[Option[A]] = new Serializer[Option[A]] {

    override def configure(map: util.Map[String, _], b: Boolean): Unit =
      serializer.configure(map, b)
    override def close(): Unit = serializer.close()

    override def serialize(topic: String, data: Option[A]): Array[Byte] =
      data.map(serializer.serialize(topic, _)).orNull

    override def serialize(topic: String,
                           headers: Headers,
                           data: Option[A]): Array[Byte] =
      data.map(serializer.serialize(topic, headers, _)).orNull
  }

  def optionDeserializer[A](
                                     deserializer: Deserializer[A]
                                   ): Deserializer[Option[A]] = new Deserializer[Option[A]] {

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit =
      deserializer.configure(configs, isKey)

    override def deserialize(topic: String, data: Array[Byte]): Option[A] =
      if (data == null) None else Some(deserializer.deserialize(topic, data))

    override def deserialize(topic: String,
                             headers: Headers,
                             data: Array[Byte]): Option[A] =
      if (data == null) None
      else Some(deserializer.deserialize(topic, headers, data))

    override def close(): Unit =
      deserializer.close()

  }

}
