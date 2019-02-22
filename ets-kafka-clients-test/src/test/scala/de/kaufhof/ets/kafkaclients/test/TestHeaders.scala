package de.kaufhof.ets.kafkaclients.test

import java.nio.charset.StandardCharsets
import java.util.UUID

import de.kaufhof.ets.kafkaclients.core.serialization.headers.{HeadersDeserializer, HeadersSerializer}
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders

case class TestHeaders(correlationId: UUID)

class TestHeadersSerializer extends HeadersSerializer[TestHeaders] {
  override def serialize(testHeaders: TestHeaders): Headers = {
    val headers = new RecordHeaders()

    headers.add("correlationId", testHeaders.correlationId.toString.getBytes(StandardCharsets.UTF_8))

    headers
  }
}

class TestHeadersDeserializer extends HeadersDeserializer[TestHeaders] {
  override def deserialize(headers: Headers): TestHeaders = {
    TestHeaders(UUID.fromString(new String(headers.lastHeader("correlationId").value(), StandardCharsets.UTF_8)))
  }
}