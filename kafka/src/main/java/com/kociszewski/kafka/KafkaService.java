package com.kociszewski.kafka;

import com.kociszewski.kafka.model.CreateMovieCommand;
import com.opencsv.CSVWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.Lifecycle;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaService {
    private final AdminClient adminClient;
    private final KafkaTemplate<String, CreateMovieCommand> kafkaTemplate;
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
//    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    public CreateTopicsResult createTopics(List<String> topics) {
        return adminClient.createTopics(topics.stream().map(topic -> new NewTopic(topic, 1, (short) 1)).collect(Collectors.toList()));
    }

    public void sendMany(List<String> topics) {
        for (int i = 0; i < topics.size(); i++) {
            log.info("Sending {} message", i);
//            int finalI = i;
//            executor.execute(() -> kafkaTemplate.send(topics.get(finalI), new CreateMovieCommand(topics.get(finalI), finalI)));
            kafkaTemplate.send(topics.get(i), new CreateMovieCommand(topics.get(i), i));
        }
    }

    public void stopConsumers() {
        kafkaListenerEndpointRegistry.getListenerContainers().forEach(Lifecycle::stop);
    }

    public void startConsumers() {
        kafkaListenerEndpointRegistry.getListenerContainers().forEach(Lifecycle::start);
    }

    public String generateCsv(List<String> records) throws IOException {
        String csvName = String.format("/home/users/mkociszewski/Pobrane/magisterka/kafka/%s", UUID.randomUUID().toString().concat(".csv"));
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvName))) {
            writer.writeAll(records.stream().map(record -> new String[]{record}).collect(Collectors.toList()));
        }
        return "@".concat(csvName);
    }
}
