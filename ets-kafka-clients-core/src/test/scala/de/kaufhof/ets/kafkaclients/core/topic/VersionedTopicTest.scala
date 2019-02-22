package de.kaufhof.ets.kafkaclients.core.topic

import org.scalatest.{FlatSpec, Matchers}

class VersionedTopicTest extends FlatSpec with Matchers {

  behavior of classOf[VersionedTopic[_]].getSimpleName

  val myEventUnversionedTopicName = "de.kaufhof.ets.kafkaclients.core.topic.MyEvent-unversioned"
  val myEventv1TopicName = "de.kaufhof.ets.kafkaclients.core.topic.MyEvent-v1"

  it should "generate the topic name for a protocol without a version tag by appending 'unversioned'" in {
    VersionedTopic[MyEvent].name shouldBe myEventUnversionedTopicName
  }

  it should "generate the topic name for an unversioned protocol" in {
    VersionedTopic[MyEvent_unversioned].name shouldBe myEventUnversionedTopicName
  }

  it should "generate the topic name for a versioned protocol" in {
    VersionedTopic[MyEvent_v1].name shouldBe myEventv1TopicName
  }


  it should "generate the topic name for a key without a version tag by removing 'Key' and appending 'unversioned'" in {
    VersionedTopic[MyEventKey].name shouldBe myEventUnversionedTopicName
  }

  it should "generate the topic name for an key protocol by removing 'Key'" in {
    VersionedTopic[MyEventKey_unversioned].name shouldBe myEventUnversionedTopicName
  }

  it should "generate the topic name for a versioned key by removing 'Key'" in {
    VersionedTopic[MyEventKey_v1].name shouldBe myEventv1TopicName
  }


  it should "generate the topic name for a key without a version tag by removing 'Value' and appending 'unversioned'" in {
    VersionedTopic[MyEventValue].name shouldBe myEventUnversionedTopicName
  }

  it should "generate the topic name for an key protocol by removing 'Value'" in {
    VersionedTopic[MyEventValue_unversioned].name shouldBe myEventUnversionedTopicName
  }

  it should "generate the topic name for a versioned key by removing 'Value'" in {
    VersionedTopic[MyEventValue_v1].name shouldBe myEventv1TopicName
  }

}
