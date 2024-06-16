package com.examples.kafkastreams.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class TotalValidationCount {

  private long totalValid;
  private long totalInvalid;
}
