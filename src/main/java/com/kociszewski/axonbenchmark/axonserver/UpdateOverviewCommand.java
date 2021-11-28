package com.kociszewski.axonbenchmark.axonserver;

import lombok.Value;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

@Value
public class UpdateOverviewCommand {
    @TargetAggregateIdentifier
    String movieId;
    String overview;
}
