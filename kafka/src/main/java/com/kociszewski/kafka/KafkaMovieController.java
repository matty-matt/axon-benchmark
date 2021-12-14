package com.kociszewski.kafka;

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@RestController
@RequiredArgsConstructor
@RequestMapping("/kafka")
@Slf4j
public class KafkaMovieController {
    private final AdminClient adminClient;
    private final KafkaTemplate<String, CreateMovieCommand> kafkaTemplate;
    private final MovieConsumerFactory consumerFactory;
    private final ExecutorService executor = Executors.newFixedThreadPool(8);

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
        adminClient.createTopics(uuids.stream().map(uuid -> new NewTopic(uuid, 1, (short) 1)).collect(Collectors.toList()));

        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        log.info("FINISHED, Time elapsed: {}ms", timeElapsed);

        String csvName = String.format("/home/users/mkociszewski/Pobrane/magisterka/kafka/%s", UUID.randomUUID().toString().concat(".csv"));
        try (CSVWriter writer = new CSVWriter(new FileWriter(csvName))) {
            writer.writeAll(uuids.stream().map(uuid -> new String[]{uuid}).collect(Collectors.toList()));
        }
        return "@".concat(csvName);
    }

    @PostMapping("/consume")
    public void consume(@RequestParam("file") MultipartFile file) throws IOException, CsvException {
        long start = System.currentTimeMillis();

        CSVReader csvReader = new CSVReader(new InputStreamReader(file.getInputStream()));
        List<String> uuids = csvReader.readAll().stream().map(uuid -> uuid[0]).collect(Collectors.toList());

        for (String uuid : uuids) {
            executor.execute(()->consumerFactory.createAndRun(uuid));
        }

        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        log.info("FINISHED, Time elapsed: {}ms", timeElapsed);
    }

    @PostMapping("/movies")
    public void putMovies(@RequestParam("file") MultipartFile file) throws IOException, CsvException {
        long start = System.currentTimeMillis();

        CSVReader csvReader = new CSVReader(new InputStreamReader(file.getInputStream()));
        List<String> uuids = csvReader.readAll().stream().map(uuid -> uuid[0]).collect(Collectors.toList());

        for (int i = 0; i < uuids.size(); i++) {
            kafkaTemplate.send(uuids.get(i), new CreateMovieCommand(uuids.get(i), i));
        }
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        log.info("FINISHED, Time elapsed: {}ms", timeElapsed);
    }
}
