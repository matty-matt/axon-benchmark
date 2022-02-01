package com.kociszewski.axonserver;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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

    @PostMapping("/movies/{iterations}")
    public void putAndConsumeMovies(@PathVariable int iterations) {
        long start = System.currentTimeMillis();

        List<String[]> uuids = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            String uuid = UUID.randomUUID().toString();
            uuids.add(new String[]{uuid});
            commandGateway.send(new CreateMovieCommand(uuid, i));
            if (i % 10_000 == 0) {
                long soFar = System.currentTimeMillis();
                long timeElapsedSoFar = soFar - start;
                log.info("Iterations: {}, Time elapsed so far: {}ms", i, timeElapsedSoFar);
            }
        }
        long finish = System.currentTimeMillis();
        long timeElapsed = finish - start;
        log.info("FINISHED, Time elapsed: {}ms", timeElapsed);
    }
}
