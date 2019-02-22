package de.kaufhof.ets.kafkaclients.avro

import de.kaufhof.ets.kafkaclients.core.serialization.protocol.KafkaMessage.{DeletedState, EventWithKey, EventWithoutKey, NewState}
import de.kaufhof.ets.kafkaclients.core.topic.VersionedTopic
import de.kaufhof.ets.kafkaclients.test.event.withkey.{AvroTopicDefinitionTestEventKey_v1, AvroTopicDefinitionTestEventValue_v1}
import de.kaufhof.ets.kafkaclients.test.event.withoutkey.AvroTopicDefinitionTestWithoutKeyEventValue_v1
import de.kaufhof.ets.kafkaclients.test.state.{AvroTopicDefinitionTestStateKey_v1, AvroTopicDefinitionTestStateValue_v1}
import de.kaufhof.ets.kafkaclients.test.{KafkaTestHelper, TestHeaders, TestHeadersDeserializer, TestHeadersSerializer}
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimitedTests}
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FlatSpec, Matchers}

class AvroTopicDefinitionFactoryTest extends FlatSpec with Matchers with TimeLimitedTests with KafkaTestHelper {

  override def timeLimit: Span = Span(10, Seconds)
  override val defaultTestSignaler: Signaler = ThreadSignaler

  behavior of "AvroTopicDefinitionFactory"

  val testValues = List("a", "b", "c")

  it should "work with state topics" in {
    val avroTopicDefinition = AvroTopicDefinitionFactory.Versioned.stateTopicDefinition[AvroTopicDefinitionTestStateKey_v1, AvroTopicDefinitionTestStateValue_v1, TestHeaders](new TestHeadersDeserializer, new TestHeadersSerializer)
    
    val msgs =
      testValues.map(v => NewState(AvroTopicDefinitionTestStateKey_v1("0000", v), AvroTopicDefinitionTestStateValue_v1("foo", List.empty), headers)) ++
        testValues.map(v => DeletedState(AvroTopicDefinitionTestStateKey_v1("0000", v), headers))

    produceAndConsume(msgs, avroTopicDefinition) should contain theSameElementsAs (msgs)
  }

  it should "work with eventWithKey topics" in {
    val avroTopicDefinition = AvroTopicDefinitionFactory.Versioned.eventWithKeyTopicDefinition[AvroTopicDefinitionTestEventKey_v1, AvroTopicDefinitionTestEventValue_v1, TestHeaders](new TestHeadersDeserializer, new TestHeadersSerializer)

    val msgs =
      testValues.map(v => EventWithKey(AvroTopicDefinitionTestEventKey_v1("0000", v), AvroTopicDefinitionTestEventValue_v1("foo", List.empty), headers))

    produceAndConsume(msgs, avroTopicDefinition) should contain theSameElementsAs (msgs)

  }

  it should "work with eventWithoutKey topics" in {
    val avroTopicDefinition = AvroTopicDefinitionFactory.Versioned.eventWithoutKeyTopicDefinition[AvroTopicDefinitionTestWithoutKeyEventValue_v1, TestHeaders](new TestHeadersDeserializer, new TestHeadersSerializer)

    val msgs =
      testValues.map(v => EventWithoutKey(AvroTopicDefinitionTestWithoutKeyEventValue_v1("0000", v, "foo", List.empty), headers))

    produceAndConsume(msgs, avroTopicDefinition) should contain theSameElementsAs (msgs)
  }

}
