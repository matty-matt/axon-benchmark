package com.kociszewski.kafka;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
public class Movie {
    private String movieId;

    private String title;
    private String overview;
    private String originalLanguage;
    private double vote;
    private double runtime;
    private List<String> genres;
    private boolean watched;
}
