package com.examples.kafkastreams.dto;

import lombok.Data;

@Data
public class ValidationResult {

  private String fileId;
  private int paymentId;
  private boolean valid;
  // Getters and setters
}