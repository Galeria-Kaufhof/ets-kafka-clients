package de.kaufhof.ets.kafkaclients.core.record;

import de.kaufhof.ets.kafkaclients.core.topic.TopicName;
import de.kaufhof.ets.kafkaclients.core.topic.VersionedTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;

public class KafkaProducerRecord<K, V> {

    private final String topic;
    private Integer partition;
    private Long timestamp;
    private K key;
    private V value;
    private Headers headers;

    public KafkaProducerRecord(TopicName topic, Headers headers) {
        this.topic = topic.name();
        this.headers = headers;
    }

    public KafkaProducerRecord<K, V> withKey(K key) {
        this.key = key;
        return this;
    }

    public KafkaProducerRecord<K, V> withValue(V value) {
        this.value = value;
        return this;
    }

    public ProducerRecord<K, V> build() {
        return new ProducerRecord<>(topic, partition, timestamp, key, value, headers);
    }
}
