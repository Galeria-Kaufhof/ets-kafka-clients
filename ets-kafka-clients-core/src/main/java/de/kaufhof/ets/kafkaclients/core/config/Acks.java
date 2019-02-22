package de.kaufhof.ets.kafkaclients.core.config;

public enum Acks {
    NONE("0"), ONE("1"), ALL("all");

    public final String configValue;

    Acks(String configValue) {
        this.configValue = configValue;
    }
}
