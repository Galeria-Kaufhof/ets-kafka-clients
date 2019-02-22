package de.kaufhof.ets.kafkaclients.avro.serialization

import java.nio.charset.Charset

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import de.kaufhof.ets.kafkaclients.core.record.{DeleteState, NewState, StateValue}
import org.apache.avro.specific.SpecificRecordBase
import org.apache.kafka.common.serialization.Serializer

import scala.reflect.ClassTag

/**
  * Serialize to Avro JSON Strings using UTF-8, if the data is `NewState`, else null.
  */
class AvroJsonOrNullSerializer[T <: SpecificRecordBase](codec: Injection[T, String])(implicit classTag: ClassTag[T])
  extends Serializer[StateValue[T]] {

  def this()(implicit classTag: ClassTag[T]) = this(SpecificAvroCodecs.toJson[T])

  private val charset = Charset.forName("UTF-8")

  override def close(): Unit = ()

  override def configure(configs: java.util.Map[String, _], isKey: Boolean): Unit = ()

  override def serialize(topic: String, data: StateValue[T]): Array[Byte] = {
    data match {
      case NewState(value) =>
        codec(value).getBytes(charset)
      case DeleteState =>
        null
    }
  }
}
