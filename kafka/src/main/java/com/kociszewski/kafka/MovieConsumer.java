package com.kociszewski.kafka;

import com.kociszewski.kafka.model.CreateMovieCommand;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MovieConsumer {
    @KafkaListener(
            topicPattern = ".*",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(CreateMovieCommand command) {
        log.info("Read, record={}", command);
    }
}
