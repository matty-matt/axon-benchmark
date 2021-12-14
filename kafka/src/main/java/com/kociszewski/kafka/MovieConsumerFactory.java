package com.kociszewski.kafka;

import com.kociszewski.kafka.model.CreateMovieCommand;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Component
@Slf4j
public class MovieConsumerFactory {
    @Value(value = "${kafka.bootstrap-address}")
    private String bootstrapAddress;

    public void createAndRun(String topic) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        properties.put(JsonDeserializer.TRUSTED_PACKAGES, "com.kociszewski.kafka.model");

        KafkaConsumer<String, CreateMovieCommand> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));

        try (consumer) {
            while (true) {
                ConsumerRecords<String, CreateMovieCommand> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    log.info("Przeczytanko, record={}, consumer={}", record.value(), consumer.hashCode());
                });
            }
        }
    }

}
