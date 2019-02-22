package de.kaufhof.ets.kafkaclients.core.record

/**
  * Describes a value of a record of a state topic.
  * Topics are subdivided into
  * - Event topics
  * - State topics
  *
  * Event topics are subdivided into
  * - Event topics with records without keys but values, exclusively
  * - Event topics with records with keys and values, exclusively
  *
  * State topics always contain keys but two types of data
  * - New state, which means a new state for the given key exists
  * - Delete state, which means the state for the given key was deleted
  *
  * @tparam V Type of value of the updated state
  */
sealed trait StateValue[+V]
case class NewState[V](value: V) extends StateValue[V]
case object DeleteState extends StateValue[Nothing]
