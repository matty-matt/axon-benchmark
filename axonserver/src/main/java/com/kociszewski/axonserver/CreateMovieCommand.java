package com.kociszewski.axonserver;

import lombok.Value;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

@Value
public class CreateMovieCommand {
    @TargetAggregateIdentifier
    String movieId;
    int iter;
}
