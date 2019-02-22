package de.kaufhof.ets.kafkaclients.core.consumerproducer

import java.util.concurrent.atomic.AtomicReference

import de.kaufhof.ets.kafkaclients.akkastream.AkkaStreamsConsumerAndProducerTest
import de.kaufhof.ets.kafkaclients.core.config._
import de.kaufhof.ets.kafkaclients.core.serialization.protocol.KafkaMessage._
import de.kaufhof.ets.kafkaclients.core.serialization.protocol._
import de.kaufhof.ets.kafkaclients.core.topic.VersionedTopic
import de.kaufhof.ets.kafkaclients.test.{KafkaConfigTestHelper, TestHeaders, TestHeadersDeserializer, TestHeadersSerializer}
import de.kaufhof.ets.kafkaclients.test.state.TestState_v1
import org.scalatest.concurrent.{Eventually, Signaler, ThreadSignaler, TimeLimitedTests}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class SimpleConsumerProducerTest extends FlatSpec with Matchers with TimeLimitedTests with KafkaConfigTestHelper with Eventually {

  override def timeLimit: Span = Span(10, Seconds)
  override val defaultTestSignaler: Signaler = ThreadSignaler

  behavior of "simple consumer and producer"

  val topic = VersionedTopic[TestState_v1]

  def defaultProducerConfig(serviceName: String): KafkaProducerConfig = {
    KafkaProducerConfig(
      bootstrapServers = bootstrapServers,
      RandomClientId(serviceName)
    )
  }

  def defaultConsumerConfig(serviceName: String, offsetStrategy: OffsetStrategy = FromBeginning): KafkaConsumerConfig = {
    KafkaConsumerConfig(
      bootstrapServers = bootstrapServers,
      SimpleGroupId(serviceName),
      offsetStrategy.strategy,
      RandomClientId(serviceName)
    )
  }

  def producer[K, V, H, Msg <: KafkaMessage[K, V, H]](topicDef: KafkaTopicDefinition[K, V, H, Msg]) = {
    new SimpleKafkaProducer[K, V, H, Msg](defaultProducerConfig("testservice"), topicDef)
  }

  def consumer[K, V, H, Msg <: KafkaMessage[K, V, H]](topicDef: KafkaTopicDefinition[K, V, H, Msg]) = {
    new SimpleKafkaConsumer[K, V, H, Msg](defaultConsumerConfig("testservice"), topicDef)
  }

  def produceAndConsume[K, V, H, Msg <: KafkaMessage[K, V, H]](msgs: List[Msg], topicDef: KafkaTopicDefinition[K, V, H, Msg]): AtomicReference[List[Msg]]   = {
    val p: SimpleKafkaProducer[K, V, H, Msg] = producer(topicDef)
    val c: SimpleKafkaConsumer[K, V, H, Msg] = consumer(topicDef)

    val consumedMsgs = new AtomicReference[List[Msg]](List.empty)

    Await.result(Future.traverse(msgs)(p.produce), 10.seconds)

    c.startReading(msg => consumedMsgs.updateAndGet((msgs: List[Msg]) => msg :: msgs))

    consumedMsgs
  }

  val testValues = List("a", "b", "c")

  it should "work with state topics" in {
    val msgs =
      testValues.map(v => NewState(StateTopicKey_v1(v), StateTopicValue_v1(v), headers)) ++
        testValues.map(v => DeletedState(StateTopicKey_v1(v), headers))

    val res =
      produceAndConsume(msgs, TestStateTopicDefinition)

    eventually(timeout(10.seconds), interval(100.millis)) {
      res.get() should contain theSameElementsAs (msgs)
    }
  }
}

case class StateTopicKey_v1(k: String)
case class StateTopicValue_v1(v: String)

case class EventWithKeyTopicKey_v1(k: String)
case class EventWithKeyTopicValue_v1(v: String)

case class EventWithoutKeyTopicValue_v1(v: String)

case object TestStateTopicDefinition extends VersionedKafkaTopicDefinition[
  StateTopicKey_v1, StateTopicValue_v1, TestHeaders, StateMessage[StateTopicKey_v1, StateTopicValue_v1, TestHeaders]] {
  override def protocol: KafkaMessageProtocol[StateTopicKey_v1, StateTopicValue_v1, TestHeaders, StateMessage[StateTopicKey_v1, StateTopicValue_v1, TestHeaders]] =
    KafkaStateProtocol(
      AkkaStreamsConsumerAndProducerTest.testDeserializer(StateTopicKey_v1.apply),
      AkkaStreamsConsumerAndProducerTest.testSerializer[StateTopicKey_v1](_.k),
      AkkaStreamsConsumerAndProducerTest.testDeserializer(StateTopicValue_v1.apply),
      AkkaStreamsConsumerAndProducerTest.testSerializer[StateTopicValue_v1](_.v),
      new TestHeadersDeserializer,
      new TestHeadersSerializer
    )
}

case object TestEventWithKeyTopicDefinition extends VersionedKafkaTopicDefinition[
  EventWithKeyTopicKey_v1, EventWithKeyTopicValue_v1, TestHeaders, EventWithKey[EventWithKeyTopicKey_v1, EventWithKeyTopicValue_v1, TestHeaders]] {
  override def protocol: KafkaMessageProtocol[
    EventWithKeyTopicKey_v1, EventWithKeyTopicValue_v1, TestHeaders,
    EventWithKey[EventWithKeyTopicKey_v1, EventWithKeyTopicValue_v1, TestHeaders]] =
    KafkaEventWithKeyProtocol(
      AkkaStreamsConsumerAndProducerTest.testDeserializer(EventWithKeyTopicKey_v1.apply),
      AkkaStreamsConsumerAndProducerTest.testSerializer[EventWithKeyTopicKey_v1](_.k),
      AkkaStreamsConsumerAndProducerTest.testDeserializer(EventWithKeyTopicValue_v1.apply),
      AkkaStreamsConsumerAndProducerTest.testSerializer[EventWithKeyTopicValue_v1](_.v),
      new TestHeadersDeserializer,
      new TestHeadersSerializer
    )
}

case object TestEventWithoutKeyTopicDefinition extends VersionedKafkaTopicDefinition[Nothing, EventWithoutKeyTopicValue_v1, TestHeaders, EventWithoutKey[EventWithoutKeyTopicValue_v1, TestHeaders]] {
  override def protocol: KafkaMessageProtocol[Nothing, EventWithoutKeyTopicValue_v1, TestHeaders, EventWithoutKey[EventWithoutKeyTopicValue_v1, TestHeaders]] =
    KafkaEventWithoutKeyProtocol(
      AkkaStreamsConsumerAndProducerTest.testDeserializer(
        EventWithoutKeyTopicValue_v1.apply),
      AkkaStreamsConsumerAndProducerTest.testSerializer[EventWithoutKeyTopicValue_v1](_.v),
      new TestHeadersDeserializer,
      new TestHeadersSerializer
    )
}
