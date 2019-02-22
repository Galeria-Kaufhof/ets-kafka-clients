package de.kaufhof.ets.kafkaclients.avro.serialization

import java.nio.charset.Charset

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.avro.specific.SpecificRecordBase
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer

import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/**
  * Deserialize from Avro JSON Strings using UTF-8.
  */
class AvroJsonDeserializer[T <: SpecificRecordBase](codec: Injection[T, String])(implicit classTag: ClassTag[T])
  extends Deserializer[T] {

  def this()(implicit classTag: ClassTag[T]) = this(SpecificAvroCodecs.toJson[T])

  private val charset = Charset.forName("UTF-8")

  override def close(): Unit = ()

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): T = {
    codec.invert(new String(data, charset)) match {
      case Success(value) =>
        value
      case Failure(exception) =>
        throw new SerializationException(s"Error when deserializing byte[] to record", exception)
    }
  }
}
