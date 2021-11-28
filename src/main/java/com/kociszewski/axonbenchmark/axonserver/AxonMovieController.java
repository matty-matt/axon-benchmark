package com.kociszewski.axonbenchmark.axonserver;

import com.opencsv.CSVWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/axon")
@Slf4j
public class AxonMovieController {

    private final CommandGateway commandGateway;

    @PostMapping("/movies")
    public void putMovies() throws IOException {
        List<String[]> uuids = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            String uuid = UUID.randomUUID().toString();
            uuids.add(new String[]{uuid});
            commandGateway.send(new CreateMovieCommand(uuid));
        }

        try (CSVWriter writer = new CSVWriter(
                new FileWriter(String.format("/home/users/mkociszewski/Pobrane/magisterka/axonserver/%s.csv", UUID.randomUUID())))) {
            writer.writeAll(uuids);
        }

    }
}
