package com.kociszewski.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Component
public class MovieConsumerFactory {

    public void createAndRun(String topic) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new JsonDeserializer<>(Movie.class));

        KafkaConsumer<String, Movie> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));
        try (consumer) {
            while (true) {
                ConsumerRecords<String, Movie> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> System.out.println("Przeczytanko " + record.value()));
            }
        }
    }

}
