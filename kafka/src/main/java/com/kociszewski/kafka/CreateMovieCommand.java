package com.kociszewski.kafka;

import lombok.Value;

@Value
public class CreateMovieCommand {
    String movieId;
    int iter;
}
