package com.kociszewski.axonbenchmark.axonserver;

import lombok.Value;

@Value
public class MovieCreatedEvent {
    String movieId;
    int iter;
}
