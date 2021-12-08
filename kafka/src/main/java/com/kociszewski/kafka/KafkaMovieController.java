package com.kociszewski.kafka;

import com.opencsv.CSVWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/kafka")
@Slf4j
public class KafkaMovieController {
    public static final long ITERATIONS = 10;
    private final AdminClient adminClient;

    @PostMapping("/movies")
    public void putMovies() throws IOException {
        long start = System.currentTimeMillis();

        List<String[]> uuids = new ArrayList<>();
        for (int i = 0; i < ITERATIONS; i++) {
            String uuid = UUID.randomUUID().toString();
            uuids.add(new String[]{uuid});
            // TODO wrzutka do kafki
            adminClient.createTopics(Collections.singletonList(new NewTopic(uuid, 1, (short) 1)));

            if (i % 10_000 == 0) {
                long soFar = System.currentTimeMillis();
                long timeElapsedSoFar = soFar - start;
                log.info("Iterations: {}, Time elapsed so far: {}ms", i, timeElapsedSoFar);
            }
        }
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        log.info("FINISHED, Time elapsed: {}ms", timeElapsed);

        try (CSVWriter writer = new CSVWriter(
                new FileWriter(String.format("/home/users/mkociszewski/Pobrane/magisterka/kafka/%s.csv", UUID.randomUUID())))) {
            writer.writeAll(uuids);
        }
    }
}
