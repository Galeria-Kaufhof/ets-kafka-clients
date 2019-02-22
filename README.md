# ETS Kafka Clients

Kafka client library that adds de/serialization and versioning semantics to the official Kafka Clients library.

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