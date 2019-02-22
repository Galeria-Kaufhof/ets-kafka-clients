# ETS Kafka Clients

Kafka client library that adds de/serialization and versioning semantics to the official Kafka Clients library.

## Modules

Currently, you can use four different modules:

* ets-kafka-clients-akka-stream
* ets-kafka-clients-avro
* ets-kafka-clients-core
* ets-kafka-clients-test

### ets-kafka-clients-akka-stream

This module contains helper to create [Akka Stream Kafka](https://doc.akka.io/docs/akka-stream-kafka/current/) `Flow`s and `Source`s.

### ets-kafka-clients-avro

This module contains de/serializers for `Avro` using `Twitter Bijection`.

### ets-kafka-clients-core

This module contains the core functionality like different `KafkaMessageProtocol`s, configuration wrappers and a simple consumer and producer.

### ets-kafka-clients-test

You can find examples at the `ets-kafka-clients-test` module.

## De/serialization

This library introduces three types of topics, these are

* Event topics with keys
* Event topics without keys
* State topics

### Event topics

These topics contain events like `OrderCreatedEvent` or `OrderUpdatedEvent`.

By **event** we mean that something occurred that may change some state.

### State topics

These topics contain states like `OrderState`.

By **state** we mean the current state of an entity.

**State topics** always contain keys but two types of data
  * New state, which means a new state for the given key exists
  * Delete state, which means the state for the given key was deleted

## Versioning

When using the `Versioned` classes and functions, topics have to be named like `.*-(unversioned|v[0-9]+)`.
