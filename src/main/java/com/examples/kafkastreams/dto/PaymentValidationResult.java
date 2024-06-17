package com.examples.kafkastreams.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PaymentValidationResult {

  private String fileId;
  private int paymentId;
  private boolean valid;
  // Getters and setters
}