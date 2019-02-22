package de.kaufhof.ets.kafkaclients.core.topic

import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

trait TopicName {
  val name: String
}

object VersionedTopic {

  private val logger = LoggerFactory.getLogger(classOf[VersionedTopic[_]])

  private val versioned = "(.*)_(v[0-9]+)$".r
  private val unversioned = "(.*)_(unversioned)$".r

  case class GivenEventName(inputClassName: String) {
    def extractEventNameAndVersion: ExtractedNameAndVersion =
      List(versioned, unversioned)
        .flatMap { regex =>
          regex.findFirstMatchIn(inputClassName).map { m =>
            val eventNameWithoutVersion = m.group(1)
            val eventVersion = m.group(2)

            ExtractedNameAndVersion(inputClassName, eventNameWithoutVersion, Some(eventVersion))
          }
        }.headOption.getOrElse(ExtractedNameAndVersion(inputClassName, inputClassName, None))
  }

  case class ExtractedNameAndVersion(inputClassName: String, eventNameWithoutVersion: String, eventVersion: Option[String]) {
    def removeKeyOrValueFromEventName: ExtractedNameAndVersion = {

      val eventNameWithoutVersionAndKeyOrValue = if (eventNameWithoutVersion.endsWith("Key")) {
        logger.warn(s"Given class $inputClassName ends with 'Key'. Please pass the value instead.")
        replaceLast(eventNameWithoutVersion, "Key", "")
      } else if (eventNameWithoutVersion.endsWith("Value")) {
        replaceLast(eventNameWithoutVersion, "Value", "")
      } else {
        logger.warn(s"Given class $inputClassName does not end with 'Value'. Please pass the value instead.")
        eventNameWithoutVersion
      }

      ExtractedNameAndVersion(inputClassName, eventNameWithoutVersionAndKeyOrValue, eventVersion)
    }
  }

  private def replaceLast(string: String, substring: String, replacement: String): String = {
    val index = string.lastIndexOf(substring)

    if (index == -1) {
      string
    } else {
      string.substring(0, index) + replacement + string.substring(index + substring.length)
    }
  }

}
case class VersionedTopic[P]()(implicit classTag: ClassTag[P]) extends TopicName {

  import VersionedTopic._

  private val eventClassName = classTag.runtimeClass.getName

  override val name: String = GivenEventName(eventClassName).extractEventNameAndVersion.removeKeyOrValueFromEventName match {
    case ExtractedNameAndVersion(_, nameWithoutVersion, Some(version)) =>
      nameWithoutVersion + "-" + version
    case ExtractedNameAndVersion(_, nameWithoutVersion, None) =>
      nameWithoutVersion + "-unversioned"
  }

  logger.debug(s"Parsed topic name is $name")

}

case class FixedTopicName(override val name: String) extends TopicName