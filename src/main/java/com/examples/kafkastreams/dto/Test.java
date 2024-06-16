package com.examples.kafkastreams.dto;

import lombok.Data;

import java.util.List;

@Data
public class Test {
    private String key;
    private List<String> words;
}
