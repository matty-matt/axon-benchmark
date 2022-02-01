package com.kociszewski.axonserver;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.spring.stereotype.Aggregate;

import java.util.List;

import static org.axonframework.modelling.command.AggregateLifecycle.apply;

@Aggregate
@NoArgsConstructor
@Slf4j
@Getter
public class Movie {
    @AggregateIdentifier
    private String movieId;

    private String title;
    private String overview;
    private String originalLanguage;
    private double vote;
    private double runtime;
    private List<String> genres;
    private boolean watched;

    @CommandHandler
    public Movie(CreateMovieCommand command) {
        apply(new MovieCreatedEvent(command.getMovieId(), command.getIter()));
    }

    @EventSourcingHandler
    private void on(MovieCreatedEvent event) {
        this.movieId = event.getMovieId();
        log.info("Read, record={}", event);
    }

    @CommandHandler
    public void handle(UpdateOverviewCommand command) {
        apply(new OverviewUpdatedEvent(command.getOverview()));
    }

    @EventSourcingHandler
    public void on(OverviewUpdatedEvent event) {
        this.overview = overview.concat(event.getOverview());
    }
}
