package de.kaufhof.ets.kafkaclients.akkastream

import java.nio.charset.StandardCharsets
import java.util

import de.kaufhof.ets.kafkaclients.core.serialization.protocol.KafkaMessage._
import de.kaufhof.ets.kafkaclients.core.serialization.protocol._
import de.kaufhof.ets.kafkaclients.test.{KafkaTestHelper, TestHeaders, TestHeadersDeserializer, TestHeadersSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimitedTests}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

class AkkaStreamsConsumerAndProducerTest extends FlatSpec with Matchers with TimeLimitedTests with KafkaTestHelper {

  override def timeLimit: Span = Span(10, Seconds)
  override val defaultTestSignaler: Signaler = ThreadSignaler

  behavior of "akka streams consumer and producer"

  val testValues = List("a", "b", "c")

  it should "work with state topics" in {
    val msgs =
      testValues.map(v => NewState(AkkaStateTopicKey_v1(v), AkkaStateTopicValue_v1(v), headers)) ++
        testValues.map(v => DeletedState(AkkaStateTopicKey_v1(v), headers))

    produceAndConsume(msgs, TestAkkaStateTopicDefinition) should contain theSameElementsAs (msgs)
  }

  it should "work with eventWithKey topics" in {
    val msgs =
      testValues.map(v => EventWithKey(AkkaEventWithKeyTopicKey_v1(v), AkkaEventWithKeyTopicValue_v1(v), headers))

    produceAndConsume(msgs, TestAkkaEventWithKeyTopicDefinition) should contain theSameElementsAs (msgs)
  }

  it should "work with eventWithoutKey topics" in {
    val msgs =
      testValues.map(v => EventWithoutKey(AkkaEventWithoutKeyTopicValue_v1(v), headers))

    produceAndConsume(msgs, TestAkkaEventWithoutKeyTopicDefinition) should contain theSameElementsAs (msgs)
  }

}

case class AkkaStateTopicKey_v1(k: String)
case class AkkaStateTopicValue_v1(v: String)

case class AkkaEventWithKeyTopicKey_v1(k: String)
case class AkkaEventWithKeyTopicValue_v1(v: String)


case class AkkaEventWithoutKeyTopicValue_v1(v: String)

case object TestAkkaStateTopicDefinition extends VersionedKafkaTopicDefinition[
      AkkaStateTopicKey_v1, AkkaStateTopicValue_v1, TestHeaders, StateMessage[AkkaStateTopicKey_v1, AkkaStateTopicValue_v1, TestHeaders]] {
  override def protocol: KafkaMessageProtocol[AkkaStateTopicKey_v1, AkkaStateTopicValue_v1, TestHeaders, StateMessage[AkkaStateTopicKey_v1, AkkaStateTopicValue_v1, TestHeaders]] =
    KafkaStateProtocol(
      AkkaStreamsConsumerAndProducerTest.testDeserializer(AkkaStateTopicKey_v1.apply),
      AkkaStreamsConsumerAndProducerTest.testSerializer[AkkaStateTopicKey_v1](_.k),
      AkkaStreamsConsumerAndProducerTest.testDeserializer(AkkaStateTopicValue_v1.apply),
      AkkaStreamsConsumerAndProducerTest.testSerializer[AkkaStateTopicValue_v1](_.v),
      new TestHeadersDeserializer,
      new TestHeadersSerializer
    )
}

case object TestAkkaEventWithKeyTopicDefinition extends VersionedKafkaTopicDefinition[
      AkkaEventWithKeyTopicKey_v1, AkkaEventWithKeyTopicValue_v1, TestHeaders, EventWithKey[AkkaEventWithKeyTopicKey_v1, AkkaEventWithKeyTopicValue_v1, TestHeaders]] {
  override def protocol: KafkaMessageProtocol[
    AkkaEventWithKeyTopicKey_v1, AkkaEventWithKeyTopicValue_v1, TestHeaders,
    EventWithKey[AkkaEventWithKeyTopicKey_v1, AkkaEventWithKeyTopicValue_v1, TestHeaders]] =
    KafkaEventWithKeyProtocol(
      AkkaStreamsConsumerAndProducerTest.testDeserializer(AkkaEventWithKeyTopicKey_v1.apply),
      AkkaStreamsConsumerAndProducerTest.testSerializer[AkkaEventWithKeyTopicKey_v1](_.k),
      AkkaStreamsConsumerAndProducerTest.testDeserializer(AkkaEventWithKeyTopicValue_v1.apply),
      AkkaStreamsConsumerAndProducerTest.testSerializer[AkkaEventWithKeyTopicValue_v1](_.v),
      new TestHeadersDeserializer,
      new TestHeadersSerializer
    )
}

case object TestAkkaEventWithoutKeyTopicDefinition extends VersionedKafkaTopicDefinition[Nothing, AkkaEventWithoutKeyTopicValue_v1, TestHeaders, EventWithoutKey[AkkaEventWithoutKeyTopicValue_v1, TestHeaders]] {
  override def protocol: KafkaMessageProtocol[Nothing, AkkaEventWithoutKeyTopicValue_v1, TestHeaders, EventWithoutKey[AkkaEventWithoutKeyTopicValue_v1, TestHeaders]] =
    KafkaEventWithoutKeyProtocol(
      AkkaStreamsConsumerAndProducerTest.testDeserializer(
        AkkaEventWithoutKeyTopicValue_v1.apply),
      AkkaStreamsConsumerAndProducerTest.testSerializer[AkkaEventWithoutKeyTopicValue_v1](_.v),
      new TestHeadersDeserializer,
      new TestHeadersSerializer
    )
}

object AkkaStreamsConsumerAndProducerTest {
  def testSerializer[A](from: A => String) = new Serializer[A] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
    override def serialize(topic: String, data: A): Array[Byte] = from(data).getBytes(StandardCharsets.UTF_8)
    override def close(): Unit = ()
  }
  def testDeserializer[A](to: String => A) = new Deserializer[A] {
    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()
    override def deserialize(topic: String, data: Array[Byte]): A = to(new String(data, StandardCharsets.UTF_8))
    override def close(): Unit = ()
  }
}
