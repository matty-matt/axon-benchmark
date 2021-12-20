package com.kociszewski.kafka;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequiredArgsConstructor
@RequestMapping("/kafka")
@Slf4j
public class KafkaMovieController {
    private final KafkaService kafkaService;

    @PostMapping("/topics/{iterations}")
    public String createTopics(@PathVariable int iterations) throws IOException {
        long start = System.currentTimeMillis();
        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            String uuid = UUID.randomUUID().toString();
            uuids.add(uuid);
            if (i % 10_000 == 0) {
                long soFar = System.currentTimeMillis();
                long timeElapsedSoFar = soFar - start;
                log.info("Iterations: {}, Time elapsed so far: {}ms", i, timeElapsedSoFar);
            }
        }
        kafkaService.createTopics(uuids);
        return kafkaService.generateCsv(uuids);
    }

    @PostMapping("/full/{iterations}")
    public String fullService(@PathVariable int iterations) throws IOException {
        kafkaService.stopConsumers();
        long start = System.currentTimeMillis();

        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            String uuid = UUID.randomUUID().toString();
            uuids.add(uuid);
            if (i % 10_000 == 0) {
                long soFar = System.currentTimeMillis();
                long timeElapsedSoFar = soFar - start;
                log.info("Iterations: {}, Time elapsed so far: {}ms", i, timeElapsedSoFar);
            }
        }
        CreateTopicsResult result = kafkaService.createTopics(uuids);
        kafkaService.sendMany(uuids);

        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        log.info("FINISHED, Time elapsed: {}ms", timeElapsed);

        result.all().whenComplete((a, b) -> {
            log.info("COMPLETE CREATING TOPICS");
            kafkaService.startConsumers();
        });

        return kafkaService.generateCsv(uuids);
    }

    @PostMapping("/movies")
    public void putMovies(@RequestParam("file") MultipartFile file) throws IOException, CsvException {
        kafkaService.stopConsumers();

        long start = System.currentTimeMillis();

        CSVReader csvReader = new CSVReader(new InputStreamReader(file.getInputStream()));
        List<String> uuids = csvReader.readAll().stream().map(uuid -> uuid[0]).collect(Collectors.toList());

        kafkaService.sendMany(uuids);
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        log.info("FINISHED, Time elapsed: {}ms", timeElapsed);
        kafkaService.startConsumers();
    }
}
